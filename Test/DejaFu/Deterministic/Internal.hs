{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE Rank2Types                #-}

-- | Concurrent monads with a fixed scheduler: internal types and
-- functions.
module Test.DejaFu.Deterministic.Internal where

import Control.DeepSeq (NFData(..))
import Control.Monad (liftM, mapAndUnzipM)
import Control.Monad.Cont (Cont, runCont)
import Control.State
import Data.List (intersect)
import Data.List.Extra
import Data.Map (Map)
import Data.Maybe (catMaybes, fromJust, isJust, isNothing)
import Test.DejaFu.STM (CTVarId, Result(..), initialCTVarId)

import qualified Data.Map as M

-- * The @Conc@ Monad

-- | The underlying monad is based on continuations over Actions.
type M n r s a = Cont (Action n r s) a

-- | CVars are represented as a reference containing a Maybe value, a
-- list of things blocked on it, and a unique numeric identifier.
type R r a = r (CVarId, Maybe a, [Block])

-- | Dict of methods for implementations to override.
type Fixed n r s = Wrapper n r (Cont (Action n r s))

-- * Running @Conc@ Computations

-- | Scheduling is done in terms of a trace of 'Action's. Blocking can
-- only occur as a result of an action, and they cover (most of) the
-- primitives of the concurrency. 'spawn' is absent as it is
-- implemented in terms of 'newEmptyCVar', 'fork', and 'putCVar'.
data Action n r s =
    AFork (Action n r s) (Action n r s)
  | forall a. APut     (R r a) a (Action n r s)
  | forall a. ATryPut  (R r a) a (Bool -> Action n r s)
  | forall a. AGet     (R r a) (a -> Action n r s)
  | forall a. ATake    (R r a) (a -> Action n r s)
  | forall a. ATryTake (R r a) (Maybe a -> Action n r s)
  | forall a. ANoTest  (M n r s a) (a -> Action n r s)
  | forall a. AAtom    (s n r a) (a -> Action n r s)
  | ANew  (CVarId -> n (Action n r s))
  | ALift (n (Action n r s))
  | AStop

-- | Every live thread has a unique identitifer.
type ThreadId = Int

-- | Every 'CVar' also has a unique identifier.
type CVarId = Int

-- | A @Scheduler@ maintains some internal state, @s@, takes the
-- 'ThreadId' of the last thread scheduled, and the list of runnable
-- threads. It produces a 'ThreadId' to schedule, and a new state.
--
-- Note: In order to prevent computation from hanging, the runtime
-- will assume that a deadlock situation has arisen if the scheduler
-- attempts to (a) schedule a blocked thread, or (b) schedule a
-- nonexistent thread. In either of those cases, the computation will
-- be halted.
type Scheduler s = s -> ThreadId -> NonEmpty ThreadId -> (ThreadId, s)

-- | One of the outputs of the runner is a @Trace@, which is a log of
-- decisions made, alternative decisions, and the action a thread took
-- in its step.
type Trace = [(Decision, [Decision], ThreadAction)]

-- | Pretty-print a trace.
showTrace :: Trace -> String
showTrace = trace "" 0 where
  trace prefix num ((Start tid,_,_):ds)    = thread prefix num ++ trace ("S" ++ show tid) 1 ds
  trace prefix num ((SwitchTo tid,_,_):ds) = thread prefix num ++ trace ("P" ++ show tid) 1 ds
  trace prefix num ((Continue,_,_):ds)     = trace prefix (num + 1) ds
  trace prefix num []                      = thread prefix num

  thread prefix num = prefix ++ replicate num '-'

-- | Scheduling decisions are based on the state of the running
-- program, and so we can capture some of that state in recording what
-- specific decision we made.
data Decision =
    Start ThreadId
  -- ^ Start a new thread, because the last was blocked (or it's the
  -- start of computation).
  | Continue
  -- ^ Continue running the last thread for another step.
  | SwitchTo ThreadId
  -- ^ Pre-empt the running thread, and switch to another.
  deriving (Eq, Show)

instance NFData Decision where
  rnf (Start    tid) = rnf tid
  rnf (SwitchTo tid) = rnf tid
  rnf Continue = ()

-- | All the actions that a thread can perform.
data ThreadAction =
    Fork ThreadId
  -- ^ Start a new thread.
  | New CVarId
  -- ^ Create a new 'CVar'.
  | Put CVarId [ThreadId]
  -- ^ Put into a 'CVar', possibly waking up some threads.
  | BlockedPut CVarId
  -- ^ Get blocked on a put.
  | TryPut CVarId Bool [ThreadId]
  -- ^ Try to put into a 'CVar', possibly waking up some threads.
  | Read CVarId
  -- ^ Read from a 'CVar'.
  | BlockedRead CVarId
  -- ^ Get blocked on a read.
  | Take CVarId [ThreadId]
  -- ^ Take from a 'CVar', possibly waking up some threads.
  | BlockedTake CVarId
  -- ^ Get blocked on a take.
  | TryTake CVarId Bool [ThreadId]
  -- ^ Try to take from a 'CVar', possibly waking up some threads.
  | STM [ThreadId]
  -- ^ An STM transaction was executed, possibly waking up some
  -- threads.
  | BlockedSTM
  -- ^ Got blocked in an STM transaction.
  | NoTest
  -- ^ A computation annotated with '_concNoTest' was executed in a
  -- single step.
  | Lift
  -- ^ Lift an action from the underlying monad. Note that the
  -- penultimate action in a trace will always be a @Lift@, this is an
  -- artefact of how the runner works.
  | Stop
  -- ^ Cease execution and terminate.
  deriving (Eq, Show)

instance NFData ThreadAction where
  rnf (TryTake c b tids) = rnf (c, b, tids)
  rnf (TryPut  c b tids) = rnf (c, b, tids)
  rnf (BlockedRead c) = rnf c
  rnf (BlockedTake c) = rnf c
  rnf (BlockedPut  c) = rnf c
  rnf (Take c tids) = rnf (c, tids)
  rnf (Put  c tids) = rnf (c, tids)
  rnf (STM  tids) = rnf tids
  rnf (Fork tid)  = rnf tid
  rnf (New  c) = rnf c
  rnf (Read c) = rnf c
  rnf ta = ta `seq` ()

-- | An indication of how a concurrent computation failed.
data Failure =
    InternalError
  -- ^ Will be raised if the scheduler does something bad. This should
  -- never arise unless you write your own, faulty, scheduler! If it
  -- does, please file a bug report.
  | Deadlock
  -- ^ The computation became blocked indefinitely on @CVar@s.
  | STMDeadlock
  -- ^ The computation became blocked indefinitely on @CTVar@s.
  | FailureInNoTest
  -- ^ A computation annotated with '_concNoTest' produced a failure,
  -- rather than a result.
  deriving (Eq, Show)

instance NFData Failure where
  rnf f = f `seq` () -- WHNF == NF

-- | Run a concurrent computation with a given 'Scheduler' and initial
-- state, returning a 'Just' if it terminates, and 'Nothing' if a
-- deadlock is detected. Also returned is the final state of the
-- scheduler, and an execution trace.
runFixed :: Monad n => Fixed n r s -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId))
         -> Scheduler g -> g -> M n r s a -> n (Either Failure a, g, Trace)
runFixed fixed runstm sched s ma = do
  ref <- newRef (wref fixed) Nothing

  let c       = ma >>= liftN fixed . writeRef (wref fixed) ref . Just . Right
  let threads = M.fromList [(0, (runCont c $ const AStop, Nothing))]

  (s', trace) <- runThreads fixed runstm sched s threads ref
  out         <- readRef (wref fixed) ref

  return (fromJust out, s', reverse trace)

-- * Running threads

-- | A @Block@ is used to determine what sort of @CVar@-related block
-- a thread is experiencing.
data Block = WaitFull ThreadId | WaitEmpty ThreadId deriving Eq

-- | A @BlockedOn@ is used to determine what sort of variable a thread
-- is blocked on.
data BlockedOn = OnCVar | OnCTVar [CTVarId] deriving Eq

-- | Determine if a thread is blocked in a certain way.
(~=) :: (a, Maybe BlockedOn) -> BlockedOn -> Bool
(_, Just OnCVar) ~= OnCVar = True
(_, Just (OnCTVar _)) ~= (OnCTVar _) = True
_ ~= _ = False

-- | Threads are represented as a tuple of (next action, is blocked).
type Threads n r s = Map ThreadId (Action n r s, Maybe BlockedOn)

-- | Run a collection of threads, until there are no threads left.
--
-- A thread is represented as a tuple of (next action, is blocked).
--
-- Note: this returns the trace in reverse order, because it's more
-- efficient to prepend to a list than append. As this function isn't
-- exposed to users of the library, this is just an internal gotcha to
-- watch out for.
runThreads :: Monad n => Fixed n r s -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId))
           -> Scheduler g -> g -> Threads n r s -> r (Maybe (Either Failure a)) -> n (g, Trace)
runThreads fixed runstm sched origg origthreads ref = go (-1, initialCTVarId, 0) [] (-1) origg origthreads where
  go lastids sofar prior g threads
    | isTerminated  = return (g, sofar)
    | isDeadlocked  = writeRef (wref fixed) ref (Just $ Left Deadlock) >> return (g, sofar)
    | isSTMLocked   = writeRef (wref fixed) ref (Just $ Left STMDeadlock) >> return (g, sofar)
    | isNonexistant = writeRef (wref fixed) ref (Just $ Left InternalError) >> return (g, sofar)
    | isBlocked     = writeRef (wref fixed) ref (Just $ Left InternalError) >> return (g, sofar)
    | otherwise = do
      stepped <- stepThread fixed runstm (fst $ fromJust thread) (sched, g) lastids chosen threads
      case stepped of
        Right (threads', lastcvid', lastctvid', lasttid', act) ->
          let sofar' = (decision, alternatives, act) : sofar
          in  go (lastcvid', lastctvid', lasttid') sofar' chosen g' threads'

        Left failure -> writeRef (wref fixed) ref (Just $ Left failure) >> return (g, sofar)

    where
      (chosen, g')  = if prior == -1 then (0, g) else sched g prior $ head runnable' :| tail runnable'
      runnable'     = M.keys runnable
      runnable      = M.filter (isNothing . snd) threads
      thread        = M.lookup chosen threads
      isBlocked     = isJust . snd $ fromJust thread
      isNonexistant = isNothing thread
      isTerminated  = 0 `notElem` M.keys threads
      isDeadlocked  = M.null runnable && ((~= OnCVar) `fmap` M.lookup 0 threads) == Just True
      isSTMLocked   = M.null runnable && ((~= OnCTVar []) `fmap` M.lookup 0 threads) == Just True

      decision
        | chosen == prior         = Continue
        | prior `elem` runnable' = SwitchTo chosen
        | otherwise              = Start chosen

      alternatives
        | chosen == prior         = map SwitchTo $ filter (/=prior) runnable'
        | prior `elem` runnable' = Continue : map SwitchTo (filter (\t -> t /= prior && t /= chosen) runnable')
        | otherwise              = map Start $ filter (/=chosen) runnable'

-- | Run a single thread one step, by dispatching on the type of
-- 'Action'.
stepThread :: Monad n => Fixed n r s -> (forall x. s n r x -> CTVarId -> n (Result x, CTVarId)) 
           -> Action n r s
           -> (Scheduler g, g) -> (CVarId, CTVarId, ThreadId) -> ThreadId -> Threads n r s -> n (Either Failure (Threads n r s, CVarId, CTVarId, ThreadId,ThreadAction))
stepThread fixed runstm action (scheduler, schedstate) (lastcvid, lastctvid, lasttid) tid threads = case action of
  AFork    a b     -> stepFork    a b
  APut     ref a c -> stepPut     ref a c
  ATryPut  ref a c -> stepTryPut  ref a c
  AGet     ref c   -> stepGet     ref c
  ATake    ref c   -> stepTake    ref c
  ATryTake ref c   -> stepTryTake ref c
  AAtom    stm c   -> stepAtom    stm c
  ANew     na      -> stepNew     na
  ALift    na      -> stepLift    na
  ANoTest  ma a    -> stepNoTest  ma a
  AStop            -> stepStop

  where
    -- | Start a new thread, assigning it the next 'ThreadId'
    stepFork a b = return $ Right (goto b tid threads', lastcvid, lastctvid, newtid, Fork newtid) where
      threads' = launch newtid a threads
      newtid   = lasttid + 1

    -- | Put a value into a @CVar@, blocking the thread until it's
    -- empty.
    stepPut ref a c = do
      (success, threads', woken) <- putIntoCVar True ref a (const c) fixed tid threads
      cvid <- getCVarId fixed ref
      return $ Right (threads', lastcvid, lastctvid, lasttid, if success then Put cvid woken else BlockedPut cvid)

    -- | Try to put a value into a @CVar@, without blocking.
    stepTryPut ref a c = do
      (success, threads', woken) <- putIntoCVar False ref a c fixed tid threads
      cvid <- getCVarId fixed ref
      return $ Right (threads', lastcvid, lastctvid, lasttid, TryPut cvid success woken)

    -- | Get the value from a @CVar@, without emptying, blocking the
    -- thread until it's full.
    stepGet ref c = do
      (cvid, val, _) <- readRef (wref fixed) ref
      case val of
        Just val' -> return $ Right (goto (c val') tid threads, lastcvid, lastctvid, lasttid, Read cvid)
        Nothing   -> do
          threads' <- block fixed ref WaitFull tid threads
          return $ Right (threads', lastcvid, lastctvid, lasttid, BlockedRead cvid)

    -- | Take the value from a @CVar@, blocking the thread until it's
    -- full.
    stepTake ref c = do
      (success, threads', woken) <- takeFromCVar True ref (c . fromJust) fixed tid threads
      cvid <- getCVarId fixed ref
      return $ Right (threads', lastcvid, lastctvid, lasttid, if success then Take cvid woken else BlockedTake cvid)

    -- | Try to take the value from a @CVar@, without blocking.
    stepTryTake ref c = do
      (success, threads', woken) <- takeFromCVar True ref c fixed tid threads
      cvid <- getCVarId fixed ref
      return $ Right (threads', lastcvid, lastctvid, lasttid, TryTake cvid success woken)

    -- | Run a STM transaction atomically.
    stepAtom stm c = do
      (res, newctvid) <- runstm stm lastctvid
      case res of
        Success touched val ->
          let (threads', woken) = wakeSTM touched threads
          in return $ Right (goto (c val) tid threads', lastcvid, newctvid, lasttid, STM woken)
        Retry touched ->
          let threads' = blockSTM touched tid threads
          in return $ Right (threads', lastcvid, newctvid, lasttid, BlockedSTM)

    -- | Create a new @CVar@, using the next 'CVarId'.
    stepNew na = do
      let newcvid = lastcvid + 1
      a <- na newcvid
      return $ Right (goto a tid threads, newcvid, lastctvid, lasttid, New newcvid)

    -- | Lift an action from the underlying monad into the @Conc@
    -- computation.
    stepLift na = do
      a <- na
      return $ Right (goto a tid threads, lastcvid, lastctvid, lasttid, Lift)

    -- | Run a computation atomically. If this fails, the entire thing fails.
    stepNoTest ma c = do
      (a, _, _) <- runFixed fixed runstm scheduler schedstate ma
      return $
        case a of
          Right a' -> Right (goto (c a') tid threads, lastcvid, lastctvid, lasttid, NoTest)
          _ -> Left FailureInNoTest

    -- | Kill the current thread.
    stepStop = return $ Right (kill tid threads, lastcvid, lastctvid, lasttid, Stop)

-- * Manipulating @CVar@s

-- | Get the ID of a CVar
getCVarId :: Monad n => Fixed n r s -> R r a -> n CVarId
getCVarId fixed ref = (\(cvid,_,_) -> cvid) `liftM` readRef (wref fixed) ref

-- | Put a value into a @CVar@, in either a blocking or nonblocking
-- way.
putIntoCVar :: Monad n
            => Bool -> R r a -> a -> (Bool -> Action n r s)
            -> Fixed n r s -> ThreadId -> Threads n r s -> n (Bool, Threads n r s, [ThreadId])
putIntoCVar blocking ref a c fixed threadid threads = do
  (cvid, val, blocks) <- readRef (wref fixed) ref

  case val of
    Just _
      | blocking -> do
        threads' <- block fixed ref WaitEmpty threadid threads
        return (False, threads', [])

      | otherwise ->
        return (False, goto (c False) threadid threads, [])

    Nothing -> do
      writeRef (wref fixed) ref (cvid, Just a, blocks)
      (threads', woken) <- wake fixed ref WaitFull threads
      return (True, goto (c True) threadid threads', woken)

-- | Take a value from a @CVar@, in either a blocking or nonblocking
-- way.
takeFromCVar :: Monad n
             => Bool -> R r a -> (Maybe a -> Action n r s)
             -> Fixed n r s -> ThreadId -> Threads n r s -> n (Bool, Threads n r s, [ThreadId])
takeFromCVar blocking ref c fixed threadid threads = do
  (cvid, val, blocks) <- readRef (wref fixed) ref

  case val of
    Just _ -> do
      writeRef (wref fixed) ref (cvid, Nothing, blocks)
      (threads', woken) <- wake fixed ref WaitEmpty threads
      return (True, goto (c val) threadid threads', woken)

    Nothing
      | blocking -> do
        threads' <- block fixed ref WaitFull threadid threads
        return (False, threads', [])

      | otherwise ->
        return (False, goto (c Nothing) threadid threads, [])

-- * Manipulating threads

-- | Replace the @Action@ of a thread.
goto :: Action n r s -> ThreadId -> Threads n r s -> Threads n r s
goto a = M.alter $ \(Just (_, b)) -> Just (a, b)

-- | Block a thread on a @CVar@.
block :: Monad n
      => Fixed n r s -> R r a -> (ThreadId -> Block) -> ThreadId -> Threads n r s -> n (Threads n r s)
block fixed ref typ tid threads = do
  (cvid, val, blocks) <- readRef (wref fixed) ref
  writeRef (wref fixed) ref (cvid, val, typ tid : blocks)
  return $ M.alter (\(Just (a, _)) -> Just (a, Just OnCVar)) tid threads

-- | Start a thread with the given ID. This must not already be in use!
launch :: ThreadId -> Action n r s -> Threads n r s -> Threads n r s
launch tid a = M.insert tid (a, Nothing)

-- | Kill a thread.
kill :: ThreadId -> Threads n r s -> Threads n r s
kill = M.delete

-- | Wake every thread blocked on a @CVar@ read/write.
wake :: Monad n
     => Fixed n r s -> R r a -> (ThreadId -> Block) -> Threads n r s -> n (Threads n r s, [ThreadId])
wake fixed ref typ m = do
  (m', woken) <- mapAndUnzipM wake' (M.toList m)

  return (M.fromList m', catMaybes woken)

  where
    wake' a@(tid, (act, Just OnCVar)) = do
      let blck = typ tid
      (cvid, val, blocks) <- readRef (wref fixed) ref

      if blck `elem` blocks
      then writeRef (wref fixed) ref (cvid, val, filter (/= blck) blocks) >> return ((tid, (act, Nothing)), Just tid)
      else return (a, Nothing)

    wake' a = return (a, Nothing)

-- | Block a thread on some 'CTVar's.
blockSTM :: [CTVarId] -> ThreadId -> Threads n r s -> Threads n r s
blockSTM blockedOn = M.alter block where
  block (Just (a, _)) = Just (a, Just $ OnCTVar blockedOn)

-- | Unblock all threads waiting on at least one of the given
-- 'CTVar's, returning the list of unblocked threads.
wakeSTM :: [CTVarId] -> Threads n r s -> (Threads n r s, [ThreadId])
wakeSTM blockedOn threads = (M.map unblock threads, M.keys $ M.filter isBlocked threads) where
  unblock thread@(a, Just (OnCTVar _))
    | isBlocked thread = (a, Nothing)
    | otherwise = thread
  unblock thread = thread

  isBlocked (_, Just (OnCTVar ctvids)) = ctvids `intersect` blockedOn /= []
  isBlocked _ = False