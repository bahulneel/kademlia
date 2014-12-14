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

(declare update-peer assign-bucket add-peer)

(defn init
  [id]
  (let [peer (peer id)]
    (-> empty-env
        (assoc :peer peer)
        (add-peer peer))))

(defn add-peer
  [env peer]
  (-> env
      (update-peer peer)
      (assign-bucket peer)))

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
        peer (get-in env [:peers h])
        bs (:buckets env)
        top-bn (dec (count bs))
        target-bn (min top-bn (:bucket peer))
        b (bs target-bn)
        bc (count b)]
    (cond
     ;; We already have this peer
     (b h) env
     ;; We have space in the next bucket
     (< bc 8) (update-in env [:buckets target-bn] conj h)
     ;; The bucket is full and is the top one
     (= top-bn target-bn) (assign-bucket (add-bucket env) peer)
     ;; Otherwise do nothing
     :else env)))

(defn update-buckets
  [env]
  (->> env :peers vals
       (reduce assign-bucket env)))

(defn find-nearest
  [env target]
  (let [{:keys [peers buckets]} env
        bucket (-> (get-in env [:peer :hash])
                   (h/distance target)
                   h/bucket
                   (min (dec (count buckets)))
                   buckets)]
    (->> bucket
         (map peers)
         (map (fn [p] (let [hash (:hash p)
                           distance (h/distance hash target)]
                       (assoc p :distance distance))))
         (sort-by :distance h/cmp))))
