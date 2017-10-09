{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE TypeFamilies #-}

-- |
-- Module      : Test.DejaFu.STM
-- Copyright   : (c) 2016 Michael Walker
-- License     : MIT
-- Maintainer  : Michael Walker <mike@barrucadu.co.uk>
-- Stability   : experimental
-- Portability : CPP, GeneralizedNewtypeDeriving, TypeFamilies
--
-- A 'MonadSTM' implementation.
module Test.DejaFu.STM
  ( -- * The @STM@ Monad
    STM

  -- * Executing Transactions
  , Result(..)
  , TTrace
  , TAction(..)
  , TVarId
  , runTransaction
  ) where

import           Control.Applicative       (Alternative(..))
import qualified Control.Concurrent.Classy as C
import           Control.Monad             (MonadPlus(..), unless)
import           Control.Monad.Catch       (MonadCatch(..), MonadThrow(..))

import           Test.DejaFu.Common
import           Test.DejaFu.STM.Internal

-- | @since unreleased
newtype STM m a = S { runSTM :: M m a } deriving (Functor, Applicative, Monad)

-- | Create a new STM continuation.
toSTM :: ((a -> STMAction m) -> STMAction m) -> STM m a
toSTM = S . cont

instance MonadThrow (STM m) where
  throwM = toSTM . const . SThrow

instance MonadCatch (STM m) where
  catch (S stm) handler = toSTM (SCatch (runSTM . handler) stm)

instance Alternative (STM m) where
  S a <|> S b = toSTM (SOrElse a b)
  empty = toSTM (const SRetry)

instance MonadPlus (STM m)

instance C.MonadSTM (STM m) where
  type TVar (STM m) = TVar m

#if MIN_VERSION_concurrency(1,2,0)
  -- retry and orElse are top-level definitions in
  -- Control.Monad.STM.Class in 1.2 and up
#else
  retry = empty
  orElse = (<|>)
#endif

  newTVarN n = toSTM . SNew n

  readTVar = toSTM . SRead

  writeTVar tvar a = toSTM (\c -> SWrite tvar a (c ()))

-- | Run a transaction, returning the result and new initial
-- 'TVarId'. If the transaction ended by calling 'retry', any 'TVar'
-- modifications are undone.
--
-- @since unreleased
runTransaction :: C.MonadConc m
  => STM m a
  -> IdSource
  -> m (Result a, IdSource, TTrace)
runTransaction ma tvid = do
  (res, undo, tvid', trace) <- doTransaction (runSTM ma) tvid
  unless (isSTMSuccess res) undo
  pure (res, tvid', trace)
