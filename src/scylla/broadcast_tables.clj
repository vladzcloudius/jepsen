(ns scylla.broadcast-tables
    "This test performs transactional appends and reads of various keys--each a
    distinct row containing a single CQL list value."
    (:refer-clojure :exclude [read])
    (:require [clojure.string :as str]
              [clojure.tools.logging :refer [info]]
              [jepsen [client :as client]
                      [checker :as checker]
                      [generator :as gen]
                      [util :as util]]
              [jepsen.tests.cycle.append :as append]
              [scylla [client :as c]]
              [qbits [alia :as a]
                     [hayt :as h]]))
  (def table (str "broadcast_kv_store"))

  (defn list_to_string
    [list]
    (if (empty? list)
      nil
      (->> (map str list)
           (str/join ","))))

  (defn string_to_list
    [string]
    (if (nil? string)
      []
      (->> (str/split string #",")
           (mapv read-string))))

  (defn single-read!
    "Takes a test, session, and a transaction with a single read mop. performs a
    single CQL select by primary key, and returns the completed txn."
    [test session k]
    (->> (a/execute session
                    (h/select table
                      (h/columns :value)
                      (h/where [[= :key (str k)]]))
                    (c/read-opts test))
          first
          :value
          string_to_list))

  (defn single-read
    "Takes a test, session, and a transaction with a single read mop. performs a
    single CQL select by primary key, and returns the completed txn."
    [test session [[f k v]]]
    [[f k (single-read! test session k)]])

  (defn single-append!
    "Takes a test, session, and a transaction with a single append mop. Performs
    the append via a CQL conditional update."
    [test session txn]
    (let [[f k v] (first txn)]
      (c/assert-applied
        (let [prev (single-read! test session k)]
          (a/execute session
            (h/update table
             (h/set-columns {:value (list_to_string (conj prev v))})
             (h/where [[= :key (str k)]])
             (h/only-if [[= :value (list_to_string prev)]]))
            (c/write-opts test)))))
    txn)

  (defn append-only?
    "Is this txn append-only?"
    [txn]
    (every? (comp #{:append} first) txn))

  (defn read-only?
    "Is this txn read-only?"
    [txn]
    (every? (comp #{:r} first) txn))

  (defn apply-txn!
    "Takes a test, a session, and a txn. Performs the txn, returning the
    completed txn."
    [test session txn]
    (if (= 1 (count txn))
      (cond (read-only?   txn) (single-read     test session txn)
            (append-only? txn) (single-append!  test session txn)
            true               (assert false "what even is this"))
      (assert false "what even is this")))

  (defrecord Client [conn]
    client/Client
    (open! [this test node]
      (assoc this :conn (c/open test node)))

    (setup! [this test])

    (invoke! [this test op]
      (let [s (:session conn)]
        (c/with-errors op #{}
          (a/execute s (h/use-keyspace :system))
          (assoc op
                 :type  :ok
                 :value (apply-txn! test s (:value op))))))

    (close! [this test]
      (c/close! conn))

    (teardown! [this test])

    client/Reusable
    (reusable? [_ _] true))

  (defn workload
    "See options for jepsen.tests.append/test"
    [opts]
    (let [opts (assoc opts
                      :consistency-models [:strict-serializable]
                      :min-txn-length 1
                      :max-txn-length 1)
          w (append/test opts)]
      (assoc w
             :client (Client. nil))
             ))
