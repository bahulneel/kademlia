(ns kademlia.hash)

(defn hash->bytes
  [h]
  (if (sequential? h)
    h
    (->> h
         (partition 2)
         (map (partial apply str))
         (map #(Integer/parseInt % 16)))))

(defn xor
  [b1 b2]
  (bit-and 0xff (bit-xor b1 b2)))

(defn distance
  [h1 h2]
  (map xor (hash->bytes h1) (hash->bytes h2)))

(defn byte->bucket
  [b]
  (->> (reverse (range 0 8))
       (map (partial bit-shift-left 1))
       (map (partial bit-and b))
       (map-indexed vector)
       (drop-while #(zero? (second %)))
       ffirst))

(defn bucket
  [d]
  (->> d
       (map byte->bucket)
       (map-indexed (fn [i v] [(* 8 i) v]))
       (drop-while #(nil? (second %)))
       first
       (apply +)))

(defn cmp
  [h1 h2]
  (or (->> (map compare (hash->bytes h1) (hash->bytes h2))
           (drop-while zero?)
           first)
      0))
