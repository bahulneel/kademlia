(ns kademlia.core
  (:require [pandect.core :refer [sha1]]
            [kademlia.hash :as h]))

(def empty-peer {})

(defn peer
  [id]
  (assoc empty-peer :id id :hash (sha1 id)))

(def empty-env
  {:peer empty-peer
   :buckets [#{}]
   :peers {}})

(defn init
  [id]
  (assoc empty-env :peer (peer id)))

(defn update-peer
  [env peer]
  (let [hash (:hash peer)
        dist (h/distance hash (get-in env [:peer :hash]))
        peer (assoc peer
               :distance dist
               :bucket (h/bucket dist))]
    (assoc-in env [:peers hash] peer)))

(defn remove-peer
  [env peer]
  (let [hash (:hash peer)
        bs (:buckets env)
        top-bn (dec (count bs))
        target-bn (min top-bn (:bucket peer))]
    (-> env
        (update-in [:peers] dissoc hash)
        (update-in [:buckets target-bn] disj hash))))

(declare assign-bucket)

(defn add-bucket
  [env]
  (let [bs (:buckets env)
        peers (map (:peers env) (peek bs))
        env (-> env
                (assoc :buckets (pop bs))
                (update-in [:buckets] conj #{} #{}))]
    (reduce assign-bucket env peers)))

(defn assign-bucket
  [env peer]
  (let [h (:hash peer)
        bs (:buckets env)
        top-bn (dec (count bs))
        target-bn (min top-bn (:bucket peer))
        b (bs target-bn)
        bc (count b)]
    (cond
     ;; We already have this peer
     (b h) env
     ;; We have space in the next bucket
     (> 8 bc) (update-in env [:buckets target-bn] conj h)
     ;; The bucket is full and is the top one
     (= top-bn target-bn) (assign-bucket (add-bucket env) peer)
     ;; Otherwise do nothig
     :else env)))

(defn update-buckets
  [env]
  (->> env :peers vals
       (sort-by :distance h/cmp)
       (reduce assign-bucket env)))
