(ns async-test.core
  (:require [clojure.core.async :as a]
            [cemerick.bandalore :as sqs]
            [cheshire.core :refer :all]))

(defn json-file->map
  [file-path]
  (parse-string (slurp file-path) true))

(defn make-sqs-client
  [config]
  (sqs/create-client (:access-key-id config) (:secret-access-key config)))

(defn make-unique-sqs-queue
  [client]
  {:pre [(= com.amazonaws.services.sqs.AmazonSQSClient (type client))]}
  (let [uniq-id-str (str (java.util.UUID/randomUUID))]
    (sqs/create-queue client uniq-id-str)))

; make-x-receive
; should return a fn that makes a channel so that they can be added to alts
; should take receiver specific config so for sqs
; and a f to run on em

(defn make-sqs-queues->channel-multiplexer
  [client & queue-urls]
  {:pre [(every? string? queue-urls)
         (= com.amazonaws.services.sqs.AmazonSQSClient (type client))]}
  (fn []
    (let [c (a/chan)
          add-to-channel (fn [v] (a/>!! c (:body v)))]
      (a/go
        (while true
          (dorun (map (sqs/deleting-consumer client add-to-channel) (mapcat (partial sqs/polling-receive client) queue-urls)))))
      c)))

(defn go
  [& channels]
  (let [run (atom true)
        shutdown (fn [] (println "closing") (map a/close! channels) (reset! run false))]
    (a/go
      (while @run
        (let [[v ch] (a/alts! channels)]
          (if (= v :sentinal)
            (shutdown)
            (println "read" v "from" ch)))))))
