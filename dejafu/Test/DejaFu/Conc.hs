{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

-- |
-- Module      : Test.DejaFu.Conc
-- Copyright   : (c) 2016 Michael Walker
-- License     : MIT
-- Maintainer  : Michael Walker <mike@barrucadu.co.uk>
-- Stability   : experimental
-- Portability : GeneralizedNewtypeDeriving, TypeFamilies
--
-- Deterministic traced execution of concurrent computations.
--
-- This works by executing the computation on a single thread, calling
-- out to the supplied scheduler after each step to determine which
-- thread runs next.
module Test.DejaFu.Conc
  ( -- * The @DejaFu@ monad transformer
    DejaFu

  -- * Executing computations
  , Failure(..)
  , MemType(..)
  , runConcurrent
  , subconcurrency

  -- * Execution traces
  , Trace
  , Decision(..)
  , ThreadId(..)
  , ThreadAction(..)
  , Lookahead(..)
  , MVarId
  , CRefId
  , MaskingState(..)
  , showTrace
  , showFail

  -- * Scheduling
  , module Test.DejaFu.Schedule
  ) where

import           Control.Exception                (MaskingState(..))
import qualified Control.Monad.Catch              as Ca
import qualified Control.Monad.IO.Class           as IO
import           Control.Monad.Trans.Class        (MonadTrans(..))
import qualified Data.Foldable                    as F
import           Test.DejaFu.Schedule

import qualified Control.Monad.Conc.Class         as C
import           Test.DejaFu.Common
import           Test.DejaFu.Conc.Internal
import           Test.DejaFu.Conc.Internal.Common
import           Test.DejaFu.STM

-- | @since unreleased
newtype DejaFu m a = C { unC :: M m a } deriving (Functor, Applicative, Monad)

toConc :: ((a -> Action m) -> Action m) -> DejaFu m a
toConc = C . cont

wrap :: (M m a -> M m a) -> DejaFu m a -> DejaFu m a
wrap f = C . f . unC

instance IO.MonadIO m => IO.MonadIO (DejaFu m) where
  liftIO ma = toConc (\c -> ALift (fmap c (IO.liftIO ma)))

instance MonadTrans DejaFu where
  lift ma = toConc (\c -> ALift (fmap c ma))

instance Ca.MonadCatch (DejaFu m) where
  catch ma h = toConc (ACatching (unC . h) (unC ma))

instance Ca.MonadThrow (DejaFu m) where
  throwM e = toConc (\_ -> AThrow e)

instance Ca.MonadMask (DejaFu m) where
  mask                mb = toConc (AMasking MaskedInterruptible   (\f -> unC $ mb $ wrap f))
  uninterruptibleMask mb = toConc (AMasking MaskedUninterruptible (\f -> unC $ mb $ wrap f))

instance Monad m => C.MonadConc (DejaFu m) where
  type MVar     (DejaFu m) = MVar m
  type CRef     (DejaFu m) = CRef m
  type Ticket   (DejaFu m) = Ticket
  type STM      (DejaFu m) = STM m
  type ThreadId (DejaFu m) = ThreadId

  -- ----------

  forkWithUnmaskN   n ma = toConc (AFork n (\umask -> runCont (unC $ ma $ wrap umask) (\_ -> AStop (pure ()))))
  forkOnWithUnmaskN n _  = C.forkWithUnmaskN n

  -- This implementation lies and returns 2 until a value is set. This
  -- will potentially avoid special-case behaviour for 1 capability,
  -- so it seems a sane choice.
  getNumCapabilities      = toConc AGetNumCapabilities
  setNumCapabilities caps = toConc (\c -> ASetNumCapabilities caps (c ()))

  myThreadId = toConc AMyTId

  yield = toConc (\c -> AYield (c ()))
  threadDelay n = toConc (\c -> ADelay n (c ()))

  -- ----------

  newCRefN n a = toConc (ANewCRef n a)

  readCRef   ref = toConc (AReadCRef    ref)
  readForCAS ref = toConc (AReadCRefCas ref)

  peekTicket' _ = _ticketVal

  writeCRef ref      a = toConc (\c -> AWriteCRef ref a (c ()))
  casCRef   ref tick a = toConc (ACasCRef ref tick a)

  atomicModifyCRef ref f = toConc (AModCRef    ref f)
  modifyCRefCAS    ref f = toConc (AModCRefCas ref f)

  -- ----------

  newEmptyMVarN n = toConc (ANewMVar n)

  putMVar  var a = toConc (\c -> APutMVar var a (c ()))
  readMVar var   = toConc (AReadMVar var)
  takeMVar var   = toConc (ATakeMVar var)

  tryPutMVar  var a = toConc (ATryPutMVar  var a)
  tryReadMVar var   = toConc (ATryReadMVar var)
  tryTakeMVar var   = toConc (ATryTakeMVar var)

  -- ----------

  throwTo tid e = toConc (\c -> AThrowTo tid e (c ()))

  -- ----------

  atomically = toConc . AAtom

-- | Run a concurrent computation with a given 'Scheduler' and initial
-- state, returning a failure reason on error. Also returned is the
-- final state of the scheduler, and an execution trace.
--
-- __Warning:__ Blocking on the action of another thread in 'liftIO'
-- cannot be detected! So if you perform some potentially blocking
-- action in a 'liftIO' the entire collection of threads may deadlock!
-- You should therefore keep @IO@ blocks small, and only perform
-- blocking operations with the supplied primitives, insofar as
-- possible.
--
-- __Note:__ In order to prevent computation from hanging, the runtime
-- will assume that a deadlock situation has arisen if the scheduler
-- attempts to (a) schedule a blocked thread, or (b) schedule a
-- nonexistent thread. In either of those cases, the computation will
-- be halted.
--
-- @since unreleased
runConcurrent :: C.MonadConc m
  => Scheduler s
  -> MemType
  -> s
  -> DejaFu m a
  -> m (Either Failure a, s, Trace)
runConcurrent sched memtype s ma = do
  (res, ctx, trace, _) <- runConcurrency sched memtype s initialIdSource 2 (unC ma)
  pure (res, cSchedState ctx, F.toList trace)

-- | Run a concurrent computation and return its result.
--
-- This can only be called in the main thread, when no other threads
-- exist. Calls to 'subconcurrency' cannot be nested. Violating either
-- of these conditions will result in the computation failing with
-- @IllegalSubconcurrency@.
--
-- @since unreleased
subconcurrency :: DejaFu m a -> DejaFu m (Either Failure a)
subconcurrency ma = toConc (ASub (unC ma))
