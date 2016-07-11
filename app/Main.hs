{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Distributed.Paxos.Core

import System.Environment

import Control.Monad
import Data.Maybe

import Control.Concurrent.Lifted

import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.MonadBaseControl ()

import System.IO

remoteTable :: RemoteTable
remoteTable = initRemoteTable

whereisRemote :: NodeId -> String -> Process (Maybe ProcessId)
whereisRemote nid sn = do
  _ <- monitorNode nid
  whereisRemoteAsync nid sn
  let pdc (WhereIsReply nsn _) = sn == nsn
  receiveWait
    [ matchIf pdc $ \(WhereIsReply _ pid) -> return pid
    , match $ \(NodeMonitorNotification {}) -> return Nothing ]

testProcess :: ProcessId -> Process ()
testProcess pid = forever $ do
  threadDelay 1000000
  send pid $ ClientCommand "Hello world"

main :: IO ()
main = do
  args <- getArgs

  hSetBuffering stderr NoBuffering

  let config = PaxosConfig
        { promiseTimeout = seconds 1
        , acceptTimeout  = seconds 1 }

  case args of
    [identity, host, port] -> do
      putStrLn "Starting server..."

      backend <- initializeBackend host port remoteTable
      node <- newLocalNode backend

      putStrLn "Finding peers..."
      peers <- findPeers backend 1000000
      let bngen n = return $ n + 1
          peerNum = length peers
      putStrLn $ "Found " ++ show peerNum ++ " peers."

      case identity of
        "proposer" ->
          runProcess node $ do
            ms <- forM peers $ flip whereisRemote "acceptor"
            spid <- getSelfPid
            _ <- spawnLocal $ testProcess spid
            startProposer config bngen $ catMaybes ms
        "acceptor" ->
          runProcess node startAcceptor
        _ -> error "Invalid identity."
    _ -> error "Invalid paramaters."

