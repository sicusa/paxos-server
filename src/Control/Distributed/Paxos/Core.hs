{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}

#define DERIVING_COMMAND deriving (Show, Generic, Typeable, Binary)

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
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Extras.Timer

type BallotNum = Word64
type BallotNumGenerator = BallotNum -> Process BallotNum
type InstanceNum = Word64

type Decree = ByteString
data Ballot = Ballot BallotNum Decree | NullBallot
  DERIVING_COMMAND

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

deriveAccessors ''Paxos

emptyPaxos :: Paxos
emptyPaxos = Paxos Nothing NullBallot Nothing

data PaxosConfig = PaxosConfig
  { promiseTimeout :: TimeInterval
  , acceptTimeout  :: TimeInterval }

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

data AcceptResult a b
  = UserError a
  | ReceivingError String
  | CompleteRes   [b]
  | IncompleteRes [b]

acceptMultiple
  :: (Serializable a, Serializable b, Serializable c)
  => Int
  -> TimeInterval
  -> (a -> Bool)
  -> (a -> Process (Either b c))
  -> Process (AcceptResult b c)
acceptMultiple repeatNum interval predicate f = do
  spid <- getSelfPid
  wpid <- spawnLocal $ do
    link spid
    sleep interval >> send spid (TimeoutNotification 1)
  v <- doReceive repeatNum []
  kill wpid ""
  return v
  where
    doReceive 0 xs = return $ CompleteRes xs
    doReceive n xs =
      receiveWait
        [ match $ \(TimeoutNotification 1) -> return $ IncompleteRes xs
        , match $ \accepted -> do
            if predicate accepted
              then do
                res <- f accepted
                either (return . UserError) (doReceive (n - 1) . (: xs)) res
              else doReceive n xs ]

proposerProcess
  :: Paxos -> PaxosConfig -> BallotNumGenerator -> [ProcessId] -> Process ()
proposerProcess p config ballotNumGen acceptors = do
  newbn <- maybe (pure 1) ballotNumGen $ p ^. lastProposed
  spid <- getSelfPid

  say $ "Sending prepare messages: " ++ show newbn
  forM_ acceptors $ flip send (Prepare spid newbn)

  let acceptorNum = length acceptors
      promTimeout = promiseTimeout config
      ppdc (PrepareResponse _ cbn _) = cbn == newbn

  say "Waiting for Prepare responses..."
  r <- acceptMultiple acceptorNum promTimeout ppdc $ \(PrepareResponse _ _ detail) ->
         case detail of
           PromiseLastVote lvb -> return $ Right lvb
           PromiseFailed   bn  -> return $ Left  bn

  let acptTimeout = acceptTimeout config
      sendAccept bn ins bd = do
        say $ "Sending Accept messages: " ++ show bn ++ ", " ++ show ins ++ "> " ++ show bd
        forM_ acceptors $ flip send (Accept spid bn ins bd)
        say $ "Waiting Accepted messages"
        let spdc (AcceptedResponse nbn nins) = nbn == bn && nins == ins
        res <- acceptMultiple acceptorNum acptTimeout spdc (const $ return $ (Right () :: Either () ()))
        case res of
          CompleteRes _ -> say "All received, succeed." >> return True
          IncompleteRes rs -> do
            say $ "Received " ++ show (length rs) ++ " messages."
            return $ length rs > acceptorNum `div` 2
          _ -> say "Failed, unexpected error!" >> return False

      acceptPhase [] = dispatchClientCommands 0
      acceptPhase bs =
        case maximum bs of
          NullBallot -> say "Accpet phase start directly." >> dispatchClientCommands 0
          Ballot bn bd -> do
            say "Decree has been determined."
            succeed <- sendAccept bn 0 bd
            when succeed $ dispatchClientCommands 1

      dispatchClientCommands ins = do
        say $ "Waiting client command: " ++ show ins
        ClientCommand decree <- expect
        say $ "Received client command: " ++ show decree
        succeed <- sendAccept newbn ins decree
        when succeed $ dispatchClientCommands (ins + 1)

  say "Received Prepare responses."
  case r of
    CompleteRes   ps -> say "All received." >> acceptPhase ps
    IncompleteRes ps -> do -- when (length ps > acceptorNum `div` 2) $ acceptPhase ps
      if length ps > acceptorNum `div` 2
        then say ("Majority received: " ++ show (length ps)) >> acceptPhase ps
        else say $ "Incomplete promises: " ++ show (length ps)
    UserError bn -> do
      say $ "Greater ballot number has been used: " ++ show bn
      proposerProcess (lastProposed ^= Just bn $ p) config ballotNumGen acceptors
    ReceivingError e -> say e
  void $ liftIO getLine

startProposer :: PaxosConfig -> BallotNumGenerator -> [ProcessId] -> Process ()
startProposer config gen ps = do
  spid <- getSelfPid
  register "proposer" spid
  proposerProcess emptyPaxos config gen ps

acceptorProcess :: Paxos -> Process ()
acceptorProcess p = expect >>= replyProposer >>= acceptorProcess
  where
    replyProposer (Prepare pid ballotNum) = do
      say $ "Received Prepare message: " ++ show ballotNum
      spid <- getSelfPid
      let respond prevote = do
            say $ "Sending Prepare response: " ++ show prevote
            send pid $ PrepareResponse spid ballotNum prevote
          prepare = do
            respond $ PromiseLastVote (p ^. prevVoted)
            return p { nextBallot_ = Just ballotNum }
      case p ^. nextBallot of
        Just pbn ->
          if pbn < ballotNum
            then prepare
            else respond (PromiseFailed pbn) >> return p
        Nothing -> prepare

    replyProposer (Accept pid bn ins decree) = do
      say $ "Received Accept message: " ++ show bn ++ ", i: " ++ show ins
      case p ^. nextBallot of
        Just pbn -> do
          if pbn == bn
            then do
              say "Sending accepted message..."
              send pid $ AcceptedResponse bn ins
              return $ p { prevVoted_ = Ballot bn decree }
            else do
              say "Inconsistent, ignored."
              return p
        Nothing -> return p

startAcceptor :: Process ()
startAcceptor = do
  spid <- getSelfPid
  register "acceptor" spid
  acceptorProcess emptyPaxos
