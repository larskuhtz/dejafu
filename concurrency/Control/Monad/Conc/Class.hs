{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

-- |
-- Module      : Control.Monad.Conc.Class
-- Copyright   : (c) 2016 Michael Walker
-- License     : MIT
-- Maintainer  : Michael Walker <mike@barrucadu.co.uk>
-- Stability   : experimental
-- Portability : CPP, FlexibleContexts, PolyKinds, RankNTypes, ScopedTypeVariables, TypeFamilies
--
-- This module captures in a typeclass the interface of concurrency
-- monads.
--
-- __Deviations:__ An instance of @MonadConc@ is not required to be an
-- instance of @MonadFix@, unlike @IO@. The @CRef@, @MVar@, and
-- @Ticket@ types are not required to be instances of @Show@ or @Eq@,
-- unlike their normal counterparts. The @threadCapability@,
-- @threadWaitRead@, @threadWaitWrite@, @threadWaitReadSTM@,
-- @threadWaitWriteSTM@, and @mkWeakThreadId@ functions are not
-- provided. The @threadDelay@ function is not required to delay the
-- thread, merely to yield it. The @BlockedIndefinitelyOnMVar@ (and
-- similar) exceptions are /not/ thrown during testing, so do not rely
-- on them at all.
module Control.Monad.Conc.Class
  ( MonadConc(..)

  -- * Threads
  , spawn
  , forkFinally
  , killThread

  -- ** Named Threads
  , forkN
  , forkOnN

  -- * Exceptions
  , throw
  , catch
  , mask
  , Ca.mask_
  , uninterruptibleMask
  , Ca.uninterruptibleMask_

  -- * Mutable State
  , newMVar
  , newMVarN
  , cas
  , peekTicket

  -- * Utilities for instance writers
  , liftedF
  , liftedFork
  ) where

-- for the class and utilities
import           Control.Exception            (AsyncException(ThreadKilled),
                                               Exception, SomeException)
import           Control.Monad.Catch          (MonadCatch, MonadMask,
                                               MonadThrow)
import qualified Control.Monad.Catch          as Ca
import           Control.Monad.STM.Class      (MonadSTM, TVar, readTVar)
import           Control.Monad.Trans.Control  (MonadTransControl, StT, liftWith)
import           Data.Proxy                   (Proxy(..))

-- for the 'IO' instance
import qualified Control.Concurrent           as IO
import qualified Control.Concurrent.STM.TVar  as IO
import qualified Control.Monad.STM            as IO
import qualified Data.Atomics                 as IO
import qualified Data.IORef                   as IO
import qualified GHC.Conc                     as IO

-- for the transformer instances
import           Control.Monad.Reader         (ReaderT)
import qualified Control.Monad.RWS.Lazy       as RL
import qualified Control.Monad.RWS.Strict     as RS
import qualified Control.Monad.State.Lazy     as SL
import qualified Control.Monad.State.Strict   as SS
import           Control.Monad.Trans          (lift)
import           Control.Monad.Trans.Identity (IdentityT)
import qualified Control.Monad.Writer.Lazy    as WL
import qualified Control.Monad.Writer.Strict  as WS

-- | @MonadConc@ is an abstraction over GHC's typical concurrency
-- abstraction. It captures the interface of concurrency monads in
-- terms of how they can operate on shared state and in the presence
-- of exceptions.
--
-- Every @MonadConc@ has an associated 'MonadSTM', transactions of
-- which can be run atomically.
--
-- @since unreleased
class ( Applicative m, Monad m
      , MonadCatch m, MonadThrow m, MonadMask m
      , MonadSTM (STM m)
      , Ord (ThreadId m), Show (ThreadId m)) => MonadConc m  where

  {-# MINIMAL
        (forkWithUnmask | forkWithUnmaskN)
      , (forkOnWithUnmask | forkOnWithUnmaskN)
      , (forkOS | forkOSN)
      , getNumCapabilities
      , setNumCapabilities
      , myThreadId
      , yield
      , (newEmptyMVar | newEmptyMVarN)
      , putMVar
      , tryPutMVar
      , readMVar
      , tryReadMVar
      , takeMVar
      , tryTakeMVar
      , (newCRef | newCRefN)
      , atomicModifyCRef
      , writeCRef
      , readForCAS
      , peekTicket'
      , casCRef
      , modifyCRefCAS
      , atomically
      , throwTo
    #-}

  -- | The associated 'MonadSTM' for this class.
  --
  -- @since 1.0.0.0
  type STM m :: * -> *

  -- | The mutable reference type, like 'MVar's. This may contain one
  -- value at a time, attempting to read or take from an \"empty\"
  -- @MVar@ will block until it is full, and attempting to put to a
  -- \"full\" @MVar@ will block until it is empty.
  --
  -- @since 1.0.0.0
  type MVar m :: * -> *

  -- | The mutable non-blocking reference type. These may suffer from
  -- relaxed memory effects if functions outside the set @newCRef@,
  -- @readCRef@, @atomicModifyCRef@, and @atomicWriteCRef@ are used.
  --
  -- @since 1.0.0.0
  type CRef m :: * -> *

  -- | When performing compare-and-swap operations on @CRef@s, a
  -- @Ticket@ is a proof that a thread observed a specific previous
  -- value.
  --
  -- @since 1.0.0.0
  type Ticket m :: * -> *

  -- | An abstract handle to a thread.
  --
  -- @since 1.0.0.0
  type ThreadId m :: *

  -- | Fork a computation to happen concurrently. Communication may
  -- happen over @MVar@s.
  --
  -- > fork ma = forkWithUnmask (const ma)
  --
  -- @since 1.0.0.0
  fork :: m () -> m (ThreadId m)
  fork ma = forkWithUnmask (const ma)

  -- | Like 'fork', but the child thread is passed a function that can
  -- be used to unmask asynchronous exceptions. This function should
  -- not be used within a 'mask' or 'uninterruptibleMask'.
  --
  -- > forkWithUnmask = forkWithUnmaskN ""
  --
  -- @since 1.0.0.0
  forkWithUnmask :: ((forall a. m a -> m a) -> m ()) -> m (ThreadId m)
  forkWithUnmask = forkWithUnmaskN ""

  -- | Like 'forkWithUnmask', but the thread is given a name which may
  -- be used to present more useful debugging information.
  --
  -- > forkWithUnmaskN _ = forkWithUnmask
  --
  -- @since 1.0.0.0
  forkWithUnmaskN :: String -> ((forall a. m a -> m a) -> m ()) -> m (ThreadId m)
  forkWithUnmaskN _ = forkWithUnmask

  -- | Fork a computation to happen on a specific processor. The
  -- specified int is the /capability number/, typically capabilities
  -- correspond to physical processors or cores but this is
  -- implementation dependent. The int is interpreted modulo to the
  -- total number of capabilities as returned by 'getNumCapabilities'.
  --
  -- > forkOn c ma = forkOnWithUnmask c (const ma)
  --
  -- @since 1.0.0.0
  forkOn :: Int -> m () -> m (ThreadId m)
  forkOn c ma = forkOnWithUnmask c (const ma)

  -- | Like 'forkWithUnmask', but the child thread is pinned to the
  -- given CPU, as with 'forkOn'.
  --
  -- > forkOnWithUnmask = forkOnWithUnmaskN ""
  --
  -- @since 1.0.0.0
  forkOnWithUnmask :: Int -> ((forall a. m a -> m a) -> m ()) -> m (ThreadId m)
  forkOnWithUnmask = forkOnWithUnmaskN ""

  -- | Like 'forkWithUnmaskN', but the child thread is pinned to the
  -- given CPU, as with 'forkOn'.
  --
  -- > forkOnWithUnmaskN _ = forkOnWithUnmask
  --
  -- @since 1.0.0.0
  forkOnWithUnmaskN :: String -> Int -> ((forall a. m a -> m a) -> m ()) -> m (ThreadId m)
  forkOnWithUnmaskN _ = forkOnWithUnmask

  -- | Fork a computation to happen in a /bound thread/, which is
  -- necessary if you need to call foreign (non-Haskell) libraries
  -- that make use of thread-local state, such as OpenGL.
  --
  -- > forkOS = forkOSN ""
  --
  -- @since unreleased
  forkOS :: m () -> m (ThreadId m)
  forkOS = forkOSN ""

  -- | Like 'forkOS', but the thread is given a name which may be used
  -- to present more useful debugging information.
  --
  -- > forkOSN _ = forkOS
  --
  -- @since undefined
  forkOSN :: String -> m () -> m (ThreadId m)
  forkOSN _ = forkOS

  -- | Get the number of Haskell threads that can run simultaneously.
  --
  -- @since 1.0.0.0
  getNumCapabilities :: m Int

  -- | Set the number of Haskell threads that can run simultaneously.
  --
  -- @since 1.0.0.0
  setNumCapabilities :: Int -> m ()

  -- | Get the @ThreadId@ of the current thread.
  --
  -- @since 1.0.0.0
  myThreadId :: m (ThreadId m)

  -- | Allows a context-switch to any other currently runnable thread
  -- (if any).
  --
  -- @since 1.0.0.0
  yield :: m ()

  -- | Yields the current thread, and optionally suspends the current
  -- thread for a given number of microseconds.
  --
  -- If suspended, there is no guarantee that the thread will be
  -- rescheduled promptly when the delay has expired, but the thread
  -- will never continue to run earlier than specified.
  --
  -- > threadDelay _ = yield
  --
  -- @since 1.0.0.0
  threadDelay :: Int -> m ()
  threadDelay _ = yield

  -- | Create a new empty @MVar@.
  --
  -- > newEmptyMVar = newEmptyMVarN ""
  --
  -- @since 1.0.0.0
  newEmptyMVar :: m (MVar m a)
  newEmptyMVar = newEmptyMVarN ""

  -- | Create a new empty @MVar@, but it is given a name which may be
  -- used to present more useful debugging information.
  --
  -- > newEmptyMVarN _ = newEmptyMVar
  --
  -- @since 1.0.0.0
  newEmptyMVarN :: String -> m (MVar m a)
  newEmptyMVarN _ = newEmptyMVar

  -- | Put a value into a @MVar@. If there is already a value there,
  -- this will block until that value has been taken, at which point
  -- the value will be stored.
  --
  -- @since 1.0.0.0
  putMVar :: MVar m a -> a -> m ()

  -- | Attempt to put a value in a @MVar@ non-blockingly, returning
  -- 'True' (and filling the @MVar@) if there was nothing there,
  -- otherwise returning 'False'.
  --
  -- @since 1.0.0.0
  tryPutMVar :: MVar m a -> a -> m Bool

  -- | Block until a value is present in the @MVar@, and then return
  -- it. This does not \"remove\" the value, multiple reads are
  -- possible.
  --
  -- @since 1.0.0.0
  readMVar :: MVar m a -> m a

  -- | Attempt to read a value from a @MVar@ non-blockingly, returning
  -- a 'Just' (and emptying the @MVar@) if there is something there,
  -- otherwise returning 'Nothing'. As with 'readMVar', this does not
  -- \"remove\" the value.
  --
  -- @since 1.1.0.0
  tryReadMVar :: MVar m a -> m (Maybe a)

  -- | Take a value from a @MVar@. This \"empties\" the @MVar@,
  -- allowing a new value to be put in. This will block if there is no
  -- value in the @MVar@ already, until one has been put.
  --
  -- @since 1.0.0.0
  takeMVar :: MVar m a -> m a

  -- | Attempt to take a value from a @MVar@ non-blockingly, returning
  -- a 'Just' (and emptying the @MVar@) if there was something there,
  -- otherwise returning 'Nothing'.
  --
  -- @since 1.0.0.0
  tryTakeMVar :: MVar m a -> m (Maybe a)

  -- | Create a new reference.
  --
  -- > newCRef = newCRefN ""
  --
  -- @since 1.0.0.0
  newCRef :: a -> m (CRef m a)
  newCRef = newCRefN ""

  -- | Create a new reference, but it is given a name which may be
  -- used to present more useful debugging information.
  --
  -- > newCRefN _ = newCRef
  --
  -- @since 1.0.0.0
  newCRefN :: String -> a -> m (CRef m a)
  newCRefN _ = newCRef

  -- | Read the current value stored in a reference.
  --
  -- > readCRef cref = readForCAS cref >>= peekTicket
  --
  -- @since 1.0.0.0
  readCRef :: CRef m a -> m a
  readCRef cref = readForCAS cref >>= peekTicket

  -- | Atomically modify the value stored in a reference. This imposes
  -- a full memory barrier.
  --
  -- @since 1.0.0.0
  atomicModifyCRef :: CRef m a -> (a -> (a, b)) -> m b

  -- | Write a new value into an @CRef@, without imposing a memory
  -- barrier. This means that relaxed memory effects can be observed.
  --
  -- @since 1.0.0.0
  writeCRef :: CRef m a -> a -> m ()

  -- | Replace the value stored in a reference, with the
  -- barrier-to-reordering property that 'atomicModifyCRef' has.
  --
  -- > atomicWriteCRef r a = atomicModifyCRef r $ const (a, ())
  --
  -- @since 1.0.0.0
  atomicWriteCRef :: CRef m a -> a -> m ()
  atomicWriteCRef r a = atomicModifyCRef r $ const (a, ())

  -- | Read the current value stored in a reference, returning a
  -- @Ticket@, for use in future compare-and-swap operations.
  --
  -- @since 1.0.0.0
  readForCAS :: CRef m a -> m (Ticket m a)

  -- | Extract the actual Haskell value from a @Ticket@.
  --
  -- The @proxy m@ is to determine the @m@ in the @Ticket@ type.
  --
  -- @since 1.0.0.0
  peekTicket' :: proxy m -> Ticket m a -> a

  -- | Perform a machine-level compare-and-swap (CAS) operation on a
  -- @CRef@. Returns an indication of success and a @Ticket@ for the
  -- most current value in the @CRef@.
  --
  -- This is strict in the \"new\" value argument.
  --
  -- @since 1.0.0.0
  casCRef :: CRef m a -> Ticket m a -> a -> m (Bool, Ticket m a)

  -- | A replacement for 'atomicModifyCRef' using a compare-and-swap.
  --
  -- This is strict in the \"new\" value argument.
  --
  -- @since 1.0.0.0
  modifyCRefCAS :: CRef m a -> (a -> (a, b)) -> m b

  -- | A variant of 'modifyCRefCAS' which doesn't return a result.
  --
  -- > modifyCRefCAS_ cref f = modifyCRefCAS cref (\a -> (f a, ()))
  --
  -- @since 1.0.0.0
  modifyCRefCAS_ :: CRef m a -> (a -> a) -> m ()
  modifyCRefCAS_ cref f = modifyCRefCAS cref (\a -> (f a, ()))

  -- | Perform an STM transaction atomically.
  --
  -- @since 1.0.0.0
  atomically :: STM m a -> m a

  -- | Read the current value stored in a @TVar@. This may be
  -- implemented differently for speed.
  --
  -- > readTVarConc = atomically . readTVar
  --
  -- @since 1.0.0.0
  readTVarConc :: TVar (STM m) a -> m a
  readTVarConc = atomically . readTVar

  -- | Throw an exception to the target thread. This blocks until the
  -- exception is delivered, and it is just as if the target thread
  -- had raised it with 'throw'. This can interrupt a blocked action.
  --
  -- @since 1.0.0.0
  throwTo :: Exception e => ThreadId m -> e -> m ()

-------------------------------------------------------------------------------
-- Utilities

-- Threads

-- | Create a concurrent computation for the provided action, and
-- return a @MVar@ which can be used to query the result.
--
-- @since 1.0.0.0
spawn :: MonadConc m => m a -> m (MVar m a)
spawn ma = do
  cvar <- newEmptyMVar
  _ <- fork $ ma >>= putMVar cvar
  pure cvar

-- | Fork a thread and call the supplied function when the thread is
-- about to terminate, with an exception or a returned value. The
-- function is called with asynchronous exceptions masked.
--
-- This function is useful for informing the parent when a child
-- terminates, for example.
--
-- @since 1.0.0.0
forkFinally :: MonadConc m => m a -> (Either SomeException a -> m ()) -> m (ThreadId m)
forkFinally action and_then =
  mask $ \restore ->
    fork $ Ca.try (restore action) >>= and_then

-- | Raise the 'ThreadKilled' exception in the target thread. Note
-- that if the thread is prepared to catch this exception, it won't
-- actually kill it.
--
-- @since 1.0.0.0
killThread :: MonadConc m => ThreadId m -> m ()
killThread tid = throwTo tid ThreadKilled

-- | Like 'fork', but the thread is given a name which may be used to
-- present more useful debugging information.
--
-- @since 1.0.0.0
forkN :: MonadConc m => String -> m () -> m (ThreadId m)
forkN name ma = forkWithUnmaskN name (const ma)

-- | Like 'forkOn', but the thread is given a name which may be used
-- to present more useful debugging information.
--
-- @since 1.0.0.0
forkOnN :: MonadConc m => String -> Int -> m () -> m (ThreadId m)
forkOnN name i ma = forkOnWithUnmaskN name i (const ma)

-- Exceptions

-- | Throw an exception. This will \"bubble up\" looking for an
-- exception handler capable of dealing with it and, if one is not
-- found, the thread is killed.
--
-- @since 1.0.0.0
throw :: (MonadConc m, Exception e) => e -> m a
throw = Ca.throwM

-- | Catch an exception. This is only required to be able to catch
-- exceptions raised by 'throw', unlike the more general
-- Control.Exception.catch function. If you need to be able to catch
-- /all/ errors, you will have to use 'IO'.
--
-- @since 1.0.0.0
catch :: (MonadConc m, Exception e) => m a -> (e -> m a) -> m a
catch = Ca.catch

-- | Executes a computation with asynchronous exceptions
-- /masked/. That is, any thread which attempts to raise an exception
-- in the current thread with 'throwTo' will be blocked until
-- asynchronous exceptions are unmasked again.
--
-- The argument passed to mask is a function that takes as its
-- argument another function, which can be used to restore the
-- prevailing masking state within the context of the masked
-- computation. This function should not be used within an
-- 'uninterruptibleMask'.
--
-- @since 1.0.0.0
mask :: MonadConc m => ((forall a. m a -> m a) -> m b) -> m b
mask = Ca.mask

-- | Like 'mask', but the masked computation is not
-- interruptible. THIS SHOULD BE USED WITH GREAT CARE, because if a
-- thread executing in 'uninterruptibleMask' blocks for any reason,
-- then the thread (and possibly the program, if this is the main
-- thread) will be unresponsive and unkillable. This function should
-- only be necessary if you need to mask exceptions around an
-- interruptible operation, and you can guarantee that the
-- interruptible operation will only block for a short period of
-- time. The supplied unmasking function should not be used within a
-- 'mask'.
--
-- @since 1.0.0.0
uninterruptibleMask :: MonadConc m => ((forall a. m a -> m a) -> m b) -> m b
uninterruptibleMask = Ca.uninterruptibleMask

-- Mutable Variables

-- | Create a new @MVar@ containing a value.
--
-- @since 1.0.0.0
newMVar :: MonadConc m => a -> m (MVar m a)
newMVar a = do
  cvar <- newEmptyMVar
  putMVar cvar a
  pure cvar

-- | Create a new @MVar@ containing a value, but it is given a name
-- which may be used to present more useful debugging information.
--
-- @since 1.0.0.0
newMVarN :: MonadConc m => String -> a -> m (MVar m a)
newMVarN n a = do
  cvar <- newEmptyMVarN n
  putMVar cvar a
  pure cvar

-- | Extract the actual Haskell value from a @Ticket@.
--
-- This doesn't do do any monadic computation, the @m@ appears in the
-- result type to determine the @m@ in the @Ticket@ type.
--
-- @since 1.0.0.0
peekTicket :: forall m a. MonadConc m => Ticket m a -> m a
peekTicket t = pure $ peekTicket' (Proxy :: Proxy m) (t :: Ticket m a)

-- | Compare-and-swap a value in a @CRef@, returning an indication of
-- success and the new value.
--
-- @since 1.0.0.0
cas :: MonadConc m => CRef m a -> a -> m (Bool, a)
cas cref a = do
  tick         <- readForCAS cref
  (suc, tick') <- casCRef cref tick a
  a'           <- peekTicket tick'

  pure (suc, a')

-------------------------------------------------------------------------------
-- Concrete instances

-- | @since 1.0.0.0
instance MonadConc IO where
  type STM      IO = IO.STM
  type MVar     IO = IO.MVar
  type CRef     IO = IO.IORef
  type Ticket   IO = IO.Ticket
  type ThreadId IO = IO.ThreadId

  fork   = IO.forkIO
  forkOn = IO.forkOn
  forkOS = IO.forkOS

  forkWithUnmaskN n ma = forkWithUnmask $ \umask -> do
    labelMe n
    ma umask

  forkOnWithUnmaskN n i ma = forkOnWithUnmask i $ \umask -> do
    labelMe n
    ma umask

  getNumCapabilities = IO.getNumCapabilities
  setNumCapabilities = IO.setNumCapabilities
  readMVar           = IO.readMVar
  tryReadMVar        = IO.tryReadMVar
  myThreadId         = IO.myThreadId
  yield              = IO.yield
  threadDelay        = IO.threadDelay
  throwTo            = IO.throwTo
  newEmptyMVar       = IO.newEmptyMVar
  putMVar            = IO.putMVar
  tryPutMVar         = IO.tryPutMVar
  takeMVar           = IO.takeMVar
  tryTakeMVar        = IO.tryTakeMVar
  newCRef            = IO.newIORef
  readCRef           = IO.readIORef
  atomicModifyCRef   = IO.atomicModifyIORef
  writeCRef          = IO.writeIORef
  atomicWriteCRef    = IO.atomicWriteIORef
  readForCAS         = IO.readForCAS
  peekTicket' _      = IO.peekTicket
  casCRef            = IO.casIORef
  modifyCRefCAS      = IO.atomicModifyIORefCAS
  atomically         = IO.atomically
  readTVarConc       = IO.readTVarIO

-- | Label the current thread, if the given label is nonempty.
labelMe :: String -> IO ()
labelMe "" = pure ()
labelMe n  = do
  tid <- myThreadId
  IO.labelThread tid n

-------------------------------------------------------------------------------
-- Transformer instances

#define INSTANCE(T,C,F)                                          \
instance C => MonadConc (T m) where                            { \
  type STM      (T m) = STM m                                  ; \
  type MVar     (T m) = MVar m                                 ; \
  type CRef     (T m) = CRef m                                 ; \
  type Ticket   (T m) = Ticket m                               ; \
  type ThreadId (T m) = ThreadId m                             ; \
                                                                 \
  fork   = liftedF F fork                                      ; \
  forkOn = liftedF F . forkOn                                  ; \
  forkOS = liftedF F forkOS                                    ; \
                                                                 \
  forkOSN = liftedF F . forkOSN                                ; \
                                                                 \
  forkWithUnmask        = liftedFork F forkWithUnmask          ; \
  forkWithUnmaskN   n   = liftedFork F (forkWithUnmaskN   n  ) ; \
  forkOnWithUnmask    i = liftedFork F (forkOnWithUnmask    i) ; \
  forkOnWithUnmaskN n i = liftedFork F (forkOnWithUnmaskN n i) ; \
                                                                 \
  getNumCapabilities = lift getNumCapabilities                 ; \
  setNumCapabilities = lift . setNumCapabilities               ; \
  myThreadId         = lift myThreadId                         ; \
  yield              = lift yield                              ; \
  threadDelay        = lift . threadDelay                      ; \
  throwTo t          = lift . throwTo t                        ; \
  newEmptyMVar       = lift newEmptyMVar                       ; \
  newEmptyMVarN      = lift . newEmptyMVarN                    ; \
  readMVar           = lift . readMVar                         ; \
  tryReadMVar        = lift . tryReadMVar                      ; \
  putMVar v          = lift . putMVar v                        ; \
  tryPutMVar v       = lift . tryPutMVar v                     ; \
  takeMVar           = lift . takeMVar                         ; \
  tryTakeMVar        = lift . tryTakeMVar                      ; \
  newCRef            = lift . newCRef                          ; \
  newCRefN n         = lift . newCRefN n                       ; \
  readCRef           = lift . readCRef                         ; \
  atomicModifyCRef r = lift . atomicModifyCRef r               ; \
  writeCRef r        = lift . writeCRef r                      ; \
  atomicWriteCRef r  = lift . atomicWriteCRef r                ; \
  readForCAS         = lift . readForCAS                       ; \
  peekTicket' _      = peekTicket' (Proxy :: Proxy m)          ; \
  casCRef r t        = lift . casCRef r t                      ; \
  modifyCRefCAS r    = lift . modifyCRefCAS r                  ; \
  atomically         = lift . atomically                       ; \
  readTVarConc       = lift . readTVarConc                     }

-- | New threads inherit the reader state of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(ReaderT r, MonadConc m, id)

-- | @since 1.0.0.0
INSTANCE(IdentityT, MonadConc m, id)

-- | New threads inherit the writer state of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(WL.WriterT w, (MonadConc m, Monoid w), fst)

-- | New threads inherit the writer state of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(WS.WriterT w, (MonadConc m, Monoid w), fst)

-- | New threads inherit the state of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(SL.StateT s, MonadConc m, fst)

-- | New threads inherit the state of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(SS.StateT s, MonadConc m, fst)

-- | New threads inherit the states of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(RL.RWST r w s, (MonadConc m, Monoid w), (\(a,_,_) -> a))

-- | New threads inherit the states of their parent, but do not
-- communicate results back.
--
-- @since 1.0.0.0
INSTANCE(RS.RWST r w s, (MonadConc m, Monoid w), (\(a,_,_) -> a))

#undef INSTANCE

-------------------------------------------------------------------------------

-- | Given a function to remove the transformer-specific state, lift
-- a function invocation.
--
-- @since 1.0.0.0
liftedF :: (MonadTransControl t, MonadConc m)
  => (forall x. StT t x -> x)
  -> (m a -> m b)
  -> t m a
  -> t m b
liftedF unst f ma = liftWith $ \run -> f (unst <$> run ma)

-- | Given a function to remove the transformer-specific state, lift
-- a @fork(on)WithUnmask@ invocation.
--
-- @since 1.0.0.0
liftedFork :: (MonadTransControl t, MonadConc m)
  => (forall x. StT t x -> x)
  -> (((forall x. m x -> m x) -> m a) -> m b)
  -> ((forall x. t m x -> t m x) -> t m a)
  -> t m b
liftedFork unst f ma = liftWith $ \run ->
  f (\unmask -> unst <$> run (ma $ liftedF unst unmask))
