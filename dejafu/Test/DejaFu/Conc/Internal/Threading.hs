{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Test.DejaFu.Conc.Internal.Threading
-- Copyright   : (c) 2016 Michael Walker
-- License     : MIT
-- Maintainer  : Michael Walker <mike@barrucadu.co.uk>
-- Stability   : experimental
-- Portability : ExistentialQuantification, RankNTypes
--
-- Operations and types for threads. This module is NOT considered to
-- form part of the public interface of this library.
module Test.DejaFu.Conc.Internal.Threading where

import           Control.Exception                (Exception, MaskingState(..),
                                                   SomeException, fromException)
import           Data.List                        (intersect)
import           Data.Map.Strict                  (Map)
import           Data.Maybe                       (fromMaybe, isJust)

import           Test.DejaFu.Common
import           Test.DejaFu.Conc.Internal.Common

import qualified Data.Map.Strict                  as M

--------------------------------------------------------------------------------
-- * Threads

-- | Threads are stored in a map index by 'ThreadId'.
type Threads m = Map ThreadId (Thread m)

-- | All the state of a thread.
data Thread m = Thread
  { _continuation :: Action m
  -- ^ The next action to execute.
  , _blocking     :: Maybe BlockedOn
  -- ^ The state of any blocks.
  , _handlers     :: [Handler m]
  -- ^ Stack of exception handlers
  , _masking      :: MaskingState
  -- ^ The exception masking state.
  }

-- | Construct a thread with just one action
mkthread :: Action m -> Thread m
mkthread c = Thread c Nothing [] Unmasked

--------------------------------------------------------------------------------
-- * Blocking

-- | A @BlockedOn@ is used to determine what sort of variable a thread
-- is blocked on.
data BlockedOn = OnMVarFull MVarId | OnMVarEmpty MVarId | OnTVar [TVarId] | OnMask ThreadId deriving Eq

-- | Determine if a thread is blocked in a certain way.
(~=) :: Thread m -> BlockedOn -> Bool
thread ~= theblock = case (_blocking thread, theblock) of
  (Just (OnMVarFull  _), OnMVarFull  _) -> True
  (Just (OnMVarEmpty _), OnMVarEmpty _) -> True
  (Just (OnTVar      _), OnTVar      _) -> True
  (Just (OnMask      _), OnMask      _) -> True
  _ -> False

--------------------------------------------------------------------------------
-- * Exceptions

-- | An exception handler.
data Handler m = forall e. Exception e => Handler (e -> MaskingState -> Action m)

-- | Propagate an exception upwards, finding the closest handler
-- which can deal with it.
propagate :: SomeException -> ThreadId -> Threads m -> Maybe (Threads m)
propagate e tid threads = case M.lookup tid threads >>= go . _handlers of
  Just (act, hs) -> Just $ except act hs tid threads
  Nothing -> Nothing

  where
    go [] = Nothing
    go (Handler h:hs) = maybe (go hs) (\act -> Just (act, hs)) $ h <$> fromException e

-- | Check if a thread can be interrupted by an exception.
interruptible :: Thread m -> Bool
interruptible thread = _masking thread == Unmasked || (_masking thread == MaskedInterruptible && isJust (_blocking thread))

-- | Register a new exception handler.
catching :: Exception e => (e -> Action m) -> ThreadId -> Threads m -> Threads m
catching h = M.adjust $ \thread ->
  let ms0 = _masking thread
      h'  = Handler $ \e ms -> (if ms /= ms0 then AResetMask False False ms0 else id) (h e)
  in thread { _handlers = h' : _handlers thread }

-- | Remove the most recent exception handler.
uncatching :: ThreadId -> Threads m -> Threads m
uncatching = M.adjust $ \thread -> thread { _handlers = tail $ _handlers thread }

-- | Raise an exception in a thread.
except :: (MaskingState -> Action m) -> [Handler m] -> ThreadId -> Threads m -> Threads m
except actf hs = M.adjust $ \thread -> thread
  { _continuation = actf (_masking thread)
  , _handlers = hs
  , _blocking = Nothing
  }

-- | Set the masking state of a thread.
mask :: MaskingState -> ThreadId -> Threads m -> Threads m
mask ms = M.adjust $ \thread -> thread { _masking = ms }

--------------------------------------------------------------------------------
-- * Manipulating threads

-- | Replace the @Action@ of a thread.
goto :: Action m -> ThreadId -> Threads m -> Threads m
goto a = M.adjust $ \thread -> thread { _continuation = a }

-- | Start a thread with the given ID, inheriting the masking state
-- from the parent thread. This ID must not already be in use!
launch :: ThreadId -> ThreadId -> ((forall b. M m b -> M m b) -> Action m) -> Threads m -> Threads m
launch parent tid a threads = launch' ms tid a threads where
  ms = fromMaybe Unmasked $ _masking <$> M.lookup parent threads

-- | Start a thread with the given ID and masking state. This must not already be in use!
launch' :: MaskingState -> ThreadId -> ((forall b. M m b -> M m b) -> Action m) -> Threads m -> Threads m
launch' ms tid a = M.insert tid thread where
  thread = Thread { _continuation = a umask, _blocking = Nothing, _handlers = [], _masking = ms }

  umask mb = resetMask True Unmasked >> mb >>= \b -> resetMask False ms >> pure b
  resetMask typ m = cont $ \k -> AResetMask typ True m $ k ()

-- | Kill a thread.
kill :: ThreadId -> Threads m -> Threads m
kill = M.delete

-- | Block a thread.
block :: BlockedOn -> ThreadId -> Threads m -> Threads m
block blockedOn = M.adjust $ \thread -> thread { _blocking = Just blockedOn }

-- | Unblock all threads waiting on the appropriate block. For 'TVar'
-- blocks, this will wake all threads waiting on at least one of the
-- given 'TVar's.
wake :: BlockedOn -> Threads m -> (Threads m, [ThreadId])
wake blockedOn threads = (unblock <$> threads, M.keys $ M.filter isBlocked threads) where
  unblock thread
    | isBlocked thread = thread { _blocking = Nothing }
    | otherwise = thread

  isBlocked thread = case (_blocking thread, blockedOn) of
    (Just (OnTVar tvids), OnTVar blockedOn') -> tvids `intersect` blockedOn' /= []
    (theblock, _) -> theblock == Just blockedOn
