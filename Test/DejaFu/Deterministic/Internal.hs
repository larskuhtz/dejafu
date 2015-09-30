{-# LANGUAGE CPP                 #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Concurrent monads with a fixed scheduler: internal types and
-- functions.
module Test.DejaFu.Deterministic.Internal
 ( -- * Execution
   runFixed
 , runFixed'

 -- * The @Conc@ Monad
 , M
 , V
 , R
 , Fixed

 -- * Primitive Actions
 , Action(..)

 -- * Identifiers
 , ThreadId
 , CVarId
 , CRefId

 -- * Memory models
 , MemType(..)

 -- * Scheduling & Traces
 , Scheduler
 , Trace
 , Decision(..)
 , ThreadAction(..)
 , Lookahead(..)
 , Trace'
 , showTrace
 , toTrace

 -- * Failures
 , Failure(..)
 ) where

import Control.Exception (MaskingState(..))
import Control.Monad.Cont (cont, runCont)
import Data.List (sort)
import Data.List.Extra
import Data.Maybe (fromJust, isJust, isNothing, listToMaybe)
import Test.DejaFu.STM (CTVarId, Result(..))
import Test.DejaFu.Internal
import Test.DejaFu.Deterministic.Internal.Common
import Test.DejaFu.Deterministic.Internal.Memory
import Test.DejaFu.Deterministic.Internal.Threading

import qualified Data.Map as M

#if __GLASGOW_HASKELL__ < 710
import Control.Applicative ((<$>), (<*>))
#endif

{-# ANN module ("HLint: ignore Use record patterns" :: String) #-}
{-# ANN module ("HLint: ignore Use const"           :: String) #-}

--------------------------------------------------------------------------------
-- * Execution

-- | Run a concurrent computation with a given 'Scheduler' and initial
-- state, returning a 'Just' if it terminates, and 'Nothing' if a
-- deadlock is detected. Also returned is the final state of the
-- scheduler, and an execution trace.
runFixed :: (Functor n, Monad n) => Fixed n r s -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId))
         -> Scheduler g -> MemType -> g -> M n r s a -> n (Either Failure a, g, Trace')
runFixed fixed runstm sched memtype s ma = (\(e,g,_,t) -> (e,g,t)) <$> runFixed' fixed runstm sched memtype s initialIdSource ma

-- | Same as 'runFixed', be parametrised by an 'IdSource'.
runFixed' :: forall n r s g a. (Functor n, Monad n)
  => Fixed n r s -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId))
  -> Scheduler g -> MemType -> g -> IdSource -> M n r s a -> n (Either Failure a, g, IdSource, Trace')
runFixed' fixed runstm sched memtype s idSource ma = do
  ref <- newRef fixed Nothing

  let c       = ma >>= liftN fixed . writeRef fixed ref . Just . Right
  let threads = launch' Unmasked 0 ((\a _ -> a) $ runCont c $ const AStop) M.empty

  (s', idSource', trace) <- runThreads fixed runstm sched memtype s threads idSource ref
  out <- readRef fixed ref

  return (fromJust out, s', idSource', reverse trace)

-- | Run a collection of threads, until there are no threads left.
--
-- Note: this returns the trace in reverse order, because it's more
-- efficient to prepend to a list than append. As this function isn't
-- exposed to users of the library, this is just an internal gotcha to
-- watch out for.
--
-- TODO: Phantom threads
runThreads :: (Functor n, Monad n) => Fixed n r s -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId))
           -> Scheduler g -> MemType -> g -> Threads n r s -> IdSource -> r (Maybe (Either Failure a)) -> n (g, IdSource, Trace')
runThreads fixed runstm sched memtype origg origthreads idsrc ref = go idsrc [] Nothing origg origthreads emptyBuffer where
  go idSource sofar prior g threads wb
    | isTerminated  = return (g, idSource, sofar)
    | isDeadlocked  = writeRef fixed ref (Just $ Left Deadlock) >> return (g, idSource, sofar)
    | isSTMLocked   = writeRef fixed ref (Just $ Left STMDeadlock) >> return (g, idSource, sofar)
    | isNonexistant = writeRef fixed ref (Just $ Left InternalError) >> return (g, idSource, sofar)
    | isBlocked     = writeRef fixed ref (Just $ Left InternalError) >> return (g, idSource, sofar)
    | otherwise = do
      stepped <- stepThread fixed runconc runstm memtype (_continuation $ fromJust thread) idSource chosen threads wb
      case stepped of
        Right (threads', idSource', act, wb') ->
          let sofar' = (decision, alternatives, act) : sofar
              threads'' = if (interruptible <$> M.lookup chosen threads') == Just True then unblockWaitingOn chosen threads' else threads'
          in  go idSource' sofar' (Just chosen) g' threads'' wb'

        Left UncaughtException
          | chosen == 0 -> writeRef fixed ref (Just $ Left UncaughtException) >> return (g, idSource, sofar)
          | otherwise ->
          let sofar' = (decision, alternatives, Killed) : sofar
              threads' = unblockWaitingOn chosen $ kill chosen threads
          in go idSource sofar' (Just chosen) g' threads' wb

        Left failure -> writeRef fixed ref (Just $ Left failure) >> return (g, idSource, sofar)

    where
      (chosen, g')  = sched g ((\p (_,_,a) -> (p,a)) <$> prior <*> listToMaybe sofar) $ unsafeToNonEmpty runnable'
      runnable'     = [(t, nextActions t) | t <- sort $ M.keys runnable]
      runnable      = M.filter (isNothing . _blocking) threads
      thread        = M.lookup chosen threads
      isBlocked     = isJust . _blocking $ fromJust thread
      isNonexistant = isNothing thread
      isTerminated  = 0 `notElem` M.keys threads
      isDeadlocked  = isLocked 0 threads && (((~= OnCVarFull  undefined) <$> M.lookup 0 threads) == Just True ||
                                           ((~=  OnCVarEmpty undefined) <$> M.lookup 0 threads) == Just True ||
                                           ((~=  OnMask      undefined) <$> M.lookup 0 threads) == Just True)
      isSTMLocked   = isLocked 0 threads && ((~=  OnCTVar    []) <$> M.lookup 0 threads) == Just True

      runconc ma i = do { (a,_,i',_) <- runFixed' fixed runstm sched SequentialConsistency g i ma; return (a,i') }

      unblockWaitingOn tid = M.map unblock where
        unblock thrd = case _blocking thrd of
          Just (OnMask t) | t == tid -> thrd { _blocking = Nothing }
          _ -> thrd

      decision
        | Just chosen == prior = Continue
        | prior `notElem` map (Just . fst) runnable' = Start chosen
        | otherwise = SwitchTo chosen

      alternatives
        | Just chosen == prior = [(SwitchTo t, na) | (t, na) <- runnable', Just t /= prior]
        | prior `notElem` map (Just . fst) runnable' = [(Start t, na) | (t, na) <- runnable', t /= chosen]
        | otherwise = [(if Just t == prior then Continue else SwitchTo t, na) | (t, na) <- runnable', t /= chosen]

      nextActions t = unsafeToNonEmpty . nextActions' . _continuation . fromJust $ M.lookup t threads
      nextActions' (AFork _ _)             = [WillFork]
      nextActions' (AMyTId _)              = [WillMyThreadId]
      nextActions' (ANew _)                = [WillNew]
      nextActions' (APut (c, _) _ k)       = WillPut c : nextActions' k
      nextActions' (ATryPut (c, _) _ _)    = [WillTryPut c]
      nextActions' (AGet (c, _) _)         = [WillRead c]
      nextActions' (ATake (c, _) _)        = [WillTake c]
      nextActions' (ATryTake (c, _) _)     = [WillTryTake c]
      nextActions' (ANewRef _)             = [WillNewRef]
      nextActions' (AReadRef (r, _) _)     = [WillReadRef r]
      nextActions' (AModRef (r, _) _ _)    = [WillModRef r]
      nextActions' (AAtom _ _)             = [WillSTM]
      nextActions' (AThrow _)              = [WillThrow]
      nextActions' (AThrowTo tid _ k)      = WillThrowTo tid : nextActions' k
      nextActions' (ACatching _ _ _)       = [WillCatching]
      nextActions' (APopCatching k)        = WillPopCatching : nextActions' k
      nextActions' (AMasking ms _ _)       = [WillSetMasking False ms]
      nextActions' (AResetMask b1 b2 ms k) = (if b1 then WillSetMasking else WillResetMasking) b2 ms : nextActions' k
      nextActions' (ALift _)               = [WillLift]
      nextActions' (ANoTest _ _)           = [WillNoTest]
      nextActions' (AKnowsAbout _ k)       = WillKnowsAbout : nextActions' k
      nextActions' (AForgets _ k)          = WillForgets : nextActions' k
      nextActions' (AAllKnown k)           = WillAllKnown : nextActions' k
      nextActions' (AStop)                 = [WillStop]

--------------------------------------------------------------------------------
-- * Single-step execution

-- | Run a single thread one step, by dispatching on the type of
-- 'Action'.
stepThread :: forall n r s. (Functor n, Monad n) => Fixed n r s
  -> (forall x. M n r s x -> IdSource -> n (Either Failure x, IdSource))
  -- ^ Run a 'MonadConc' computation atomically.
  -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId))
  -- ^ Run a 'MonadSTM' transaction atomically.
  -> MemType
  -- ^ The memory model
  -> Action n r s
  -- ^ Action to step
  -> IdSource
  -- ^ Source of fresh IDs
  -> ThreadId
  -- ^ ID of the current thread
  -> Threads n r s
  -- ^ Current state of threads
  -> WriteBuffer r
  -- ^ @CRef@ write buffer
  -> n (Either Failure (Threads n r s, IdSource, ThreadAction, WriteBuffer r))
stepThread fixed runconc runstm memtype action idSource tid threads wb = case action of
  AFork    a b     -> stepFork        a b
  AMyTId   c       -> stepMyTId       c
  APut     ref a c -> stepPut         ref a c
  ATryPut  ref a c -> stepTryPut      ref a c
  AGet     ref c   -> stepGet         ref c
  ATake    ref c   -> stepTake        ref c
  ATryTake ref c   -> stepTryTake     ref c
  AReadRef ref c   -> stepReadRef     ref c
  AModRef  ref f c -> stepModRef      ref f c
  AAtom    stm c   -> stepAtom        stm c
  ANew     na      -> stepNew         na
  ANewRef  na      -> stepNewRef      na
  ALift    na      -> stepLift        na
  AThrow   e       -> stepThrow       e
  AThrowTo t e c   -> stepThrowTo     t e c
  ACatching h ma c -> stepCatching    h ma c
  APopCatching a   -> stepPopCatching a
  AMasking m ma c  -> stepMasking     m ma c
  AResetMask b1 b2 m c -> stepResetMask b1 b2 m c
  ANoTest  ma a    -> stepNoTest      ma a
  AKnowsAbout v c  -> stepKnowsAbout  v c
  AForgets    v c  -> stepForgets v c
  AAllKnown   c    -> stepAllKnown c
  AStop            -> stepStop

  where
    -- | Start a new thread, assigning it the next 'ThreadId'
    stepFork a b = return $ Right (goto (b newtid) tid threads', idSource', Fork newtid, wb) where
      threads' = launch tid newtid a threads
      (idSource', newtid) = nextTId idSource

    -- | Get the 'ThreadId' of the current thread
    stepMyTId c = return $ Right (goto (c tid) tid threads, idSource, MyThreadId, wb)

    -- | Put a value into a @CVar@, blocking the thread until it's
    -- empty.
    stepPut cvar@(cvid, _) a c = do
      (success, threads', woken) <- putIntoCVar True cvar a (const c) fixed tid threads
      return $ Right (threads', idSource, if success then Put cvid woken else BlockedPut cvid, wb)

    -- | Try to put a value into a @CVar@, without blocking.
    stepTryPut cvar@(cvid, _) a c = do
      (success, threads', woken) <- putIntoCVar False cvar a c fixed tid threads
      return $ Right (threads', idSource, TryPut cvid success woken, wb)

    -- | Get the value from a @CVar@, without emptying, blocking the
    -- thread until it's full.
    stepGet cvar@(cvid, _) c = do
      (success, threads', _) <- readFromCVar False True cvar (c . fromJust) fixed tid threads
      return $ Right (threads', idSource, if success then Read cvid else BlockedRead cvid, wb)

    -- | Take the value from a @CVar@, blocking the thread until it's
    -- full.
    stepTake cvar@(cvid, _) c = do
      (success, threads', woken) <- readFromCVar True True cvar (c . fromJust) fixed tid threads
      return $ Right (threads', idSource, if success then Take cvid woken else BlockedTake cvid, wb)

    -- | Try to take the value from a @CVar@, without blocking.
    stepTryTake cvar@(cvid, _) c = do
      (success, threads', woken) <- readFromCVar True False cvar c fixed tid threads
      return $ Right (threads', idSource, TryTake cvid success woken, wb)

    -- | Read from a @CRef@.
    stepReadRef cref@(crid, _) c = do
      val <- readCRef fixed cref tid
      return $ Right (goto (c val) tid threads, idSource, ReadRef crid, wb)

    -- | Modify a @CRef@.
    stepModRef cref@(crid, _) f c = do
      (new, val) <- f <$> readCRef fixed cref tid
      wb' <- case memtype of
              -- Buffer and then immediately commit
              SequentialConsistency -> bufferWrite fixed wb tid cref new tid >>= \b -> commitWrite fixed b tid
              -- Add to buffer using thread id. TODO: Don't commit this when phantom threads + commit primitive
              TotalStoreOrder       -> bufferWrite fixed wb tid cref new tid >>= \b -> commitWrite fixed b tid
              -- Add to buffer using cref id. TODO: Don't commit this when phantom threads + commit primitive
              PartialStoreOrder     -> bufferWrite fixed wb crid cref new tid >>= \b -> commitWrite fixed b crid
      return $ Right (goto (c val) tid threads, idSource, ModRef crid, wb')

    -- | Run a STM transaction atomically.
    stepAtom stm c = do
      let oldctvid = _nextCTVId idSource
      (res, newctvid) <- runstm stm oldctvid
      case res of
        Success readen written val
          | any (<oldctvid) readen || any (<oldctvid) written ->
            let (threads', woken) = wake (OnCTVar written) threads
            in return $ Right (knows (map Right written) tid $ goto (c val) tid threads', idSource { _nextCTVId = newctvid }, STM woken, wb)
          | otherwise ->
           return $ Right (knows (map Right written) tid $ goto (c val) tid threads, idSource { _nextCTVId = newctvid }, FreshSTM, wb)
        Retry touched ->
          let threads' = block (OnCTVar touched) tid threads
          in return $ Right (threads', idSource { _nextCTVId = newctvid }, BlockedSTM, wb)
        Exception e -> stepThrow e

    -- | Run a subcomputation in an exception-catching context.
    stepCatching h ma c = return $ Right (threads', idSource, Catching, wb) where
      a     = runCont ma      (APopCatching . c)
      e exc = runCont (h exc) (APopCatching . c)

      threads' = M.alter (\(Just thread) -> Just $ thread { _continuation = a, _handlers = Handler e : _handlers thread }) tid threads

    -- | Pop the top exception handler from the thread's stack.
    stepPopCatching a = return $ Right (threads', idSource, PopCatching, wb) where
      threads' = M.alter (\(Just thread) -> Just $ thread { _continuation = a, _handlers = tail $_handlers thread }) tid threads

    -- | Throw an exception, and propagate it to the appropriate
    -- handler.
    stepThrow e = return $
      case propagate e . _handlers . fromJust $ M.lookup tid threads of
        Just (act, hs) ->
          let threads' = M.alter (\(Just thread) -> Just $ thread { _continuation = act, _handlers = hs }) tid threads
          in  Right (threads', idSource, Throw, wb)
        Nothing -> Left UncaughtException

    -- | Throw an exception to the target thread, and propagate it to
    -- the appropriate handler.
    stepThrowTo t e c = return $
      let threads' = goto c tid threads
          blocked = M.alter (\(Just thread) -> Just $ thread { _blocking = Just (OnMask t) }) tid threads
          interrupted act hs = M.alter (\(Just thread) -> Just $ thread { _continuation = act, _blocking = Nothing, _handlers = hs }) t
      in case M.lookup t threads of
           Just thread
             | interruptible thread -> case propagate e $ _handlers thread of
               Just (act, hs) -> Right (interrupted act hs threads', idSource, ThrowTo t, wb)
               Nothing
                 | t == 0     -> Left UncaughtException
                 | otherwise -> Right (kill t threads', idSource, ThrowTo t, wb)
             | otherwise -> Right (blocked, idSource, BlockedThrowTo t, wb)
           Nothing -> Right (threads', idSource, ThrowTo t, wb)

    -- | Execute a subcomputation with a new masking state, and give
    -- it a function to run a computation with the current masking
    -- state.
    --
    -- Explicit type sig necessary for checking in the prescence of
    -- 'umask', sadly.
    stepMasking :: MaskingState
                -> ((forall b. M n r s b -> M n r s b) -> M n r s a)
                -> (a -> Action n r s)
                -> n (Either Failure (Threads n r s, IdSource, ThreadAction, WriteBuffer r))
    stepMasking m ma c = return $ Right (threads', idSource, SetMasking False m, wb) where
      a = runCont (ma umask) (AResetMask False False m' . c)

      m' = _masking . fromJust $ M.lookup tid threads
      umask mb = resetMask True m' >> mb >>= \b -> resetMask False m >> return b
      resetMask typ mask = cont $ \k -> AResetMask typ True mask $ k ()

      threads' = M.alter (\(Just thread) -> Just $ thread { _continuation = a, _masking = m }) tid threads

    -- | Reset the masking thread of the state.
    stepResetMask b1 b2 m c = return $ Right (threads', idSource, (if b1 then SetMasking else ResetMasking) b2 m, wb) where
      threads' = M.alter (\(Just thread) -> Just $ thread { _continuation = c, _masking = m }) tid threads

    -- | Create a new @CVar@, using the next 'CVarId'.
    stepNew na = do
      let (idSource', newcvid) = nextCVId idSource
      a <- na newcvid
      return $ Right (knows [Left newcvid] tid $ goto a tid threads, idSource', New newcvid, wb)

    -- | Create a new @CRef@, using the next 'CRefId'.
    stepNewRef na = do
      let (idSource', newcrid) = nextCRId idSource
      a <- na newcrid
      return $ Right (goto a tid threads, idSource', NewRef newcrid, wb)

    -- | Lift an action from the underlying monad into the @Conc@
    -- computation.
    stepLift na = do
      a <- na
      return $ Right (goto a tid threads, idSource, Lift, wb)

    -- | Run a computation atomically. If this fails, the entire thing fails.
    stepNoTest ma c = do
      (a, idSource') <- runconc ma idSource
      return $
        case a of
          Right a' -> Right (goto (c a') tid threads, idSource', NoTest, wb)
          _ -> Left FailureInNoTest

    -- | Record that a variable is known about.
    stepKnowsAbout v c = return $ Right (knows [v] tid $ goto c tid threads, idSource, KnowsAbout, wb)

    -- | Record that a variable will never be touched again.
    stepForgets v c = return $ Right (forgets [v] tid $ goto c tid threads, idSource, Forgets, wb)

    -- | Record that all shared variables are known.
    stepAllKnown c = return $ Right (fullknown tid $ goto c tid threads, idSource, AllKnown, wb)

    -- | Kill the current thread.
    stepStop = return $ Right (kill tid threads, idSource, Stop, wb)
