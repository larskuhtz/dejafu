{-# LANGUAGE CPP #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module      : Test.DejaFu.Conc.Internal.Common
-- Copyright   : (c) 2016 Michael Walker
-- License     : MIT
-- Maintainer  : Michael Walker <mike@barrucadu.co.uk>
-- Stability   : experimental
-- Portability : CPP, ExistentialQuantification, RankNTypes
--
-- Common types and utility functions for deterministic execution of
-- 'MonadConc' implementations. This module is NOT considered to form
module Test.DejaFu.Conc.Internal.Common where

import qualified Control.Concurrent.Classy as C
import           Control.Exception         (Exception, MaskingState(..))
import           Data.Map.Strict           (Map)
import           Test.DejaFu.Common
import           Test.DejaFu.STM           (STM)

#if MIN_VERSION_base(4,9,0)
import qualified Control.Monad.Fail        as Fail
#endif

--------------------------------------------------------------------------------
-- * The @Conc@ Monad

-- | The underlying monad is based on continuations over 'Action's.
--
-- One might wonder why the return type isn't reflected in 'Action',
-- and a free monad formulation used. This would remove the need for a
-- @AStop@ actions having their parameter. However, this makes the
-- current expression of threads and exception handlers very difficult
-- (perhaps even not possible without significant reworking), so I
-- abandoned the attempt.
newtype M m a = M { runM :: (a -> Action m) -> Action m }

instance Functor (M m) where
    fmap f m = M $ \ c -> runM m (c . f)

instance Applicative (M m) where
    -- without the @AReturn@, a thread could lock up testing by
    -- entering an infinite loop (eg: @forever (return ())@)
    pure x  = M $ \c -> AReturn $ c x
    f <*> v = M $ \c -> runM f (\g -> runM v (c . g))

instance Monad (M m) where
    return  = pure
    m >>= k = M $ \c -> runM m (\x -> runM (k x) c)

#if MIN_VERSION_base(4,9,0)
    fail = Fail.fail

instance Fail.MonadFail (M m) where
#endif
    fail e = cont (\_ -> AThrow (MonadFailException e))

-- | The concurrent variable type used with the 'Conc' monad. One
-- notable difference between these and 'MVar's is that 'MVar's are
-- single-wakeup, and wake up in a FIFO order. Writing to a @MVar@
-- wakes up all threads blocked on reading it, and it is up to the
-- scheduler which one runs next. Taking from a @MVar@ behaves
-- analogously.
data MVar m a = MVar
  { _cvarId   :: MVarId
  , _cvarVal  :: C.CRef m (Maybe a)
  }

-- | The mutable non-blocking reference type. These are like 'IORef's.
--
-- @CRef@s are represented as a unique numeric identifier and a
-- reference containing (a) any thread-local non-synchronised writes
-- (so each thread sees its latest write), (b) a commit count (used in
-- compare-and-swaps), and (c) the current value visible to all
-- threads.
data CRef m a = CRef
  { _crefId   :: CRefId
  , _crefVal  :: C.CRef m (Map ThreadId a, Integer, a)
  }

-- | The compare-and-swap proof type.
--
-- @Ticket@s are represented as just a wrapper around the identifier
-- of the 'CRef' it came from, the commit count at the time it was
-- produced, and an @a@ value. This doesn't work in the source package
-- (atomic-primops) because of the need to use pointer equality. Here
-- we can just pack extra information into 'CRef' to avoid that need.
data Ticket a = Ticket
  { _ticketCRef   :: CRefId
  , _ticketWrites :: Integer
  , _ticketVal    :: a
  }

-- | Construct a continuation-passing operation from a function.
cont :: ((a -> Action m) -> Action m) -> M m a
cont = M

-- | Run a CPS computation with the given final computation.
runCont :: M m a -> (a -> Action m) -> Action m
runCont = runM

--------------------------------------------------------------------------------
-- * Primitive Actions

-- | Scheduling is done in terms of a trace of 'Action's. Blocking can
-- only occur as a result of an action, and they cover (most of) the
-- primitives of the concurrency. 'spawn' is absent as it is
-- implemented in terms of 'newEmptyMVar', 'fork', and 'putMVar'.
data Action m =
    AFork  String ((forall b. M m b -> M m b) -> Action m) (ThreadId -> Action m)
  | AMyTId (ThreadId -> Action m)

  | AGetNumCapabilities (Int -> Action m)
  | ASetNumCapabilities Int (Action m)

  | forall a. ANewMVar String (MVar m a -> Action m)
  | forall a. APutMVar     (MVar m a) a (Action m)
  | forall a. ATryPutMVar  (MVar m a) a (Bool -> Action m)
  | forall a. AReadMVar    (MVar m a) (a -> Action m)
  | forall a. ATryReadMVar (MVar m a) (Maybe a -> Action m)
  | forall a. ATakeMVar    (MVar m a) (a -> Action m)
  | forall a. ATryTakeMVar (MVar m a) (Maybe a -> Action m)

  | forall a.   ANewCRef String a (CRef m a -> Action m)
  | forall a.   AReadCRef    (CRef m a) (a -> Action m)
  | forall a.   AReadCRefCas (CRef m a) (Ticket a -> Action m)
  | forall a b. AModCRef     (CRef m a) (a -> (a, b)) (b -> Action m)
  | forall a b. AModCRefCas  (CRef m a) (a -> (a, b)) (b -> Action m)
  | forall a.   AWriteCRef   (CRef m a) a (Action m)
  | forall a.   ACasCRef     (CRef m a) (Ticket a) a ((Bool, Ticket a) -> Action m)

  | forall e.   Exception e => AThrow e
  | forall e.   Exception e => AThrowTo ThreadId e (Action m)
  | forall a e. Exception e => ACatching (e -> M m a) (M m a) (a -> Action m)
  | APopCatching (Action m)
  | forall a. AMasking MaskingState ((forall b. M m b -> M m b) -> M m a) (a -> Action m)
  | AResetMask Bool Bool MaskingState (Action m)

  | forall a. AAtom (STM m a) (a -> Action m)
  | ALift (m (Action m))
  | AYield  (Action m)
  | ADelay Int (Action m)
  | AReturn (Action m)
  | ACommit ThreadId CRefId
  | AStop (m ())

  | forall a. ASub (M m a) (Either Failure a -> Action m)
  | AStopSub (Action m)

--------------------------------------------------------------------------------
-- * Scheduling & Traces

-- | Look as far ahead in the given continuation as possible.
lookahead :: Action m -> Lookahead
lookahead (AFork _ _ _) = WillFork
lookahead (AMyTId _) = WillMyThreadId
lookahead (AGetNumCapabilities _) = WillGetNumCapabilities
lookahead (ASetNumCapabilities i _) = WillSetNumCapabilities i
lookahead (ANewMVar _ _) = WillNewMVar
lookahead (APutMVar (MVar c _) _ _) = WillPutMVar c
lookahead (ATryPutMVar (MVar c _) _ _) = WillTryPutMVar c
lookahead (AReadMVar (MVar c _) _) = WillReadMVar c
lookahead (ATryReadMVar (MVar c _) _) = WillTryReadMVar c
lookahead (ATakeMVar (MVar c _) _) = WillTakeMVar c
lookahead (ATryTakeMVar (MVar c _) _) = WillTryTakeMVar c
lookahead (ANewCRef _ _ _) = WillNewCRef
lookahead (AReadCRef (CRef r _) _) = WillReadCRef r
lookahead (AReadCRefCas (CRef r _) _) = WillReadCRefCas r
lookahead (AModCRef (CRef r _) _ _) = WillModCRef r
lookahead (AModCRefCas (CRef r _) _ _) = WillModCRefCas r
lookahead (AWriteCRef (CRef r _) _ _) = WillWriteCRef r
lookahead (ACasCRef (CRef r _) _ _ _) = WillCasCRef r
lookahead (ACommit t c) = WillCommitCRef t c
lookahead (AAtom _ _) = WillSTM
lookahead (AThrow _) = WillThrow
lookahead (AThrowTo tid _ _) = WillThrowTo tid
lookahead (ACatching _ _ _) = WillCatching
lookahead (APopCatching _) = WillPopCatching
lookahead (AMasking ms _ _) = WillSetMasking False ms
lookahead (AResetMask b1 b2 ms _) = (if b1 then WillSetMasking else WillResetMasking) b2 ms
lookahead (ALift _) = WillLiftIO
lookahead (AYield _) = WillYield
lookahead (ADelay n _) = WillThreadDelay n
lookahead (AReturn _) = WillReturn
lookahead (AStop _) = WillStop
lookahead (ASub _ _) = WillSubconcurrency
lookahead (AStopSub _) = WillStopSubconcurrency
