(ns scylla.counter
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [nemesis   :as nemesis]
             [generator :as gen]]
            [qbits.alia :as alia]
            [qbits.alia.policy.retry :as retry]
            [qbits.hayt :refer :all]
            [scylla [db :as db]])
  (:import (clojure.lang ExceptionInfo)
           (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(defrecord CQLCounterClient [tbl-created? session writec]
  client/Client

  (open! [_ test _]
    (let [cluster (alia/cluster {:contact-points (:nodes test)})
          session (alia/connect cluster)]
      (->CQLCounterClient tbl-created? session writec)))

  (setup! [_ test]
    (locking tbl-created?
      (when
       (compare-and-set! tbl-created? false true)
        (alia/execute session (create-keyspace :jepsen_keyspace
                                               (if-exists false)
                                               (with {:replication {:class :SimpleStrategy
                                                                    :replication_factor 3}})))
        (alia/execute session (use-keyspace :jepsen_keyspace))
        (alia/execute session (create-table :counters
                                            (if-exists false)
                                            (column-definitions {:id    :int
                                                                 :count    :counter
                                                                 :primary-key [:id]})
                                            (with {:compaction {:class (db/compaction-strategy)}})))
        (alia/execute session (update :counters
                                      (set-columns :count [+ 0])
                                      (where [[= :id 0]]))))))

  (invoke! [_ _ op]
    (alia/execute session (use-keyspace :jepsen_keyspace))
    (case (:f op)
      :add (try (do
                  (alia/execute session
                                (update :counters
                                        (set-columns {:count [+ (:value op)]})
                                        (where [[= :id 0]]))
                                {:consistency writec
                                 :retry-policy (retry/fallthrough-retry-policy)})
                  (assoc op :type :ok))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e))))
      :read (try
              (let [value (->> (alia/execute session
                                             (select :counters (where [[= :id 0]]))
                                             {:consistency :all
                                              :retry-policy (retry/fallthrough-retry-policy)})
                               first
                               :count)]
                (assoc op :type :ok :value value))
              (catch UnavailableException e
                (info "Not enough replicas - failing")
                (assoc op :type :fail :value (.getMessage e)))
              (catch ReadTimeoutException e
                (assoc op :type :fail :value :timed-out))
              (catch NoHostAvailableException e
                (info "All the servers are down - waiting 2s")
                (Thread/sleep 2000)
                (assoc op :type :fail :error (.getMessage e))))))

  (close! [_ _]
    (alia/shutdown session))

  (teardown! [_ _]))

(defn cql-counter-client
  "A counter implemented using CQL counters"
  ([] (->CQLCounterClient (atom false) nil :one))
  ([writec] (->CQLCounterClient (atom false) nil writec)))

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r {:type :invoke :f :read})

(defn workload
  "An increment-only counter workload."
  [opts]
  {:client    (cql-counter-client)
   :generator (->> (repeat 100 add)
                   (cons r)
                   gen/mix)
   :checker   (checker/counter)})

(defn inc-dec-workload
  "A workload which has both increments and decrements."
  [opts]
  {:client (cql-counter-client)
   :generator (->> (take 100 (cycle [add sub]))
                   (cons r)
                   gen/mix)
   :checker (checker/counter)})