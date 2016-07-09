{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}

#define DERIVING_COMMAND deriving (Generic, Typeable, Binary)

module Control.Distributed.Paxos.Core where

import GHC.Generics
import Data.Binary
import Data.Rank1Typeable
import Data.ByteString.Lazy (ByteString)

import Data.Accessor
import Data.Accessor.Template

import Control.Monad
import Control.Distributed.Process
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Async
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Extras.Internal.Types

type BallotNum = Word64
type BallotNumGenerator = BallotNum -> Process BallotNum
type InstanceNum = Word64

type Decree = ByteString
data Ballot = Ballot BallotNum Decree | NullBallot
  deriving (Generic, Typeable, Binary)

instance Eq Ballot where
  NullBallot == NullBallot = True
  Ballot n1 _ == Ballot n2 _ = n1 == n2
  _ == _ = False

instance Ord Ballot where
  compare NullBallot NullBallot = EQ
  compare NullBallot (Ballot _ _) = LT
  compare (Ballot _ _) NullBallot = GT
  compare (Ballot n1 _) (Ballot n2 _) = compare n1 n2

data Paxos = Paxos
  { lastProposed_ :: Maybe BallotNum
  , prevVoted_    :: Ballot
  , nextBallot_   :: Maybe BallotNum }

data PaxosConfig = PaxosConfig
  { promiseTimeout :: TimeInterval
  , acceptTimeout  :: TimeInterval }

deriveAccessors ''Paxos

data ProposerCommand
  = Prepare ProcessId BallotNum
  | Accept  ProcessId BallotNum InstanceNum Decree
  DERIVING_COMMAND

data PromiseDetail
  = PromiseLastVote Ballot
  | PromiseFailed BallotNum
  DERIVING_COMMAND

data PrepareResponse =
  PrepareResponse ProcessId BallotNum PromiseDetail
  DERIVING_COMMAND

data AcceptedResponse =
  AcceptedResponse BallotNum InstanceNum
  DERIVING_COMMAND

data LeaderElected = LeaderElected ProcessId
  DERIVING_COMMAND

data ClientCommand
  = ClientCommand Decree
  DERIVING_COMMAND

data AcceptTimeout = AcceptTimeout
  DERIVING_COMMAND
data AcceptResult a b
  = UserError a
  | ReceivingError String
  | CompleteRes   [b]
  | IncompleteRes [b]

waitTimeout'
  :: (Serializable a)
  => TimeInterval -> Async a -> Process (AsyncResult a)
waitTimeout' i a = do
  res <- waitTimeout i a
  case res of
    Nothing -> return AsyncPending
    Just v  -> return v

acceptMultiple
  :: (Serializable a, Serializable b, Serializable c)
  => Int
  -> TimeInterval
  -> (a -> Bool)
  -> (a -> Process (Either b c))
  -> Process (AcceptResult b c)
acceptMultiple repeatNum interval predicate f = do
  recAsync <- async $ task (doReceive repeatNum [])
  res <- waitTimeout' interval recAsync
  case res of
    AsyncDone v -> return $ fromRes CompleteRes v
    AsyncPending -> do
      Just apid <- resolve recAsync
      send apid AcceptTimeout
      res' <- wait recAsync
      case res' of
        AsyncDone v -> return $ fromRes IncompleteRes v
        _           -> return $ ReceivingError $ "Process failed after timeout."
    AsyncFailed reason ->
      return $ ReceivingError $ "Failed to wait messages: " ++ show reason
    AsyncLinkFailed reason ->
      return $ ReceivingError $ "Failed to link accepting process: " ++ show reason
    _ -> error $ "THIS IS IMPOSSIBLE!!!!"
  where
    doReceive 0 xs = return $ Right xs
    doReceive n xs =
      receiveWait
        [ match $ \AcceptTimeout -> return $ Right xs
        , match $ \accepted -> do
            if predicate accepted
              then do
                res <- f accepted
                either (return . Left) (doReceive (n - 1) . (: xs)) res
              else doReceive n xs ]
    fromRes = either UserError

proposerProcess
  :: Paxos -> PaxosConfig -> BallotNumGenerator -> [ProcessId] -> Process ()
proposerProcess p config ballotNumGen acceptors = do
  newbn <- maybe (pure 1) ballotNumGen $ p ^. lastProposed
  spid <- getSelfPid
  forM_ acceptors $ flip send (Prepare spid newbn)

  let acceptorNum = length acceptors
      promTimeout = promiseTimeout config
      ppdc (PrepareResponse _ cbn _) = cbn == newbn

  r <- acceptMultiple acceptorNum promTimeout ppdc $ \(PrepareResponse _ _ detail) ->
         case detail of
           PromiseLastVote lvb -> return $ Right lvb
           PromiseFailed   bn  -> return $ Left  bn

  let acptTimeout = acceptTimeout config
      sendAccept bn ins bd = do
        let spdc (AcceptedResponse nbn nins) = nbn == newbn && nins == ins
        forM_ acceptors $ flip send (Accept spid bn 0 bd)
        res <- acceptMultiple acceptorNum acptTimeout spdc (const $ return $ (Right () :: Either () ()))
        case res of
          CompleteRes _ -> return True
          IncompleteRes rs -> return $ length rs > acceptorNum `div` 2
          _ -> return False

      acceptPhase [] = dispatchClientCommands 0
      acceptPhase bs =
        case maximum bs of
          NullBallot -> dispatchClientCommands 0
          Ballot bn bd -> do
            succeed <- sendAccept bn 0 bd
            when succeed $ dispatchClientCommands 1

      dispatchClientCommands ins = do
        ClientCommand decree <- expect
        succeed <- sendAccept newbn ins decree
        when succeed $ dispatchClientCommands ins

  case r of
    CompleteRes   ps -> acceptPhase ps
    IncompleteRes ps -> when (length ps > acceptorNum `div` 2) $ acceptPhase ps
    UserError     bn ->
      proposerProcess (lastProposed ^= Just bn $ p) config ballotNumGen acceptors
    ReceivingError e -> say e

acceptorProcess :: Paxos -> Process ()
acceptorProcess p = expect >>= replyProposer >>= acceptorProcess
  where
    replyProposer (Prepare pid ballot) = do
      spid <- getSelfPid
      let respond = send pid . PrepareResponse spid ballot
          prepare = do
            respond $ PromiseLastVote (p ^. prevVoted)
            return p { nextBallot_ = Just ballot }
      case p ^. nextBallot of
        Just pbn ->
          if pbn < ballot
            then prepare
            else respond (PromiseFailed ballot) >> return p
        Nothing -> prepare

    replyProposer (Accept pid ballot ins decree) = do
      case p ^. nextBallot of
        Just pbn -> do
          if pbn == ballot
            then do
              send pid $ AcceptedResponse ballot ins
              return $ p { prevVoted_ = Ballot ballot decree }
            else return p
        Nothing -> return p
