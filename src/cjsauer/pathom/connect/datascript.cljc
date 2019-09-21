(ns cjsauer.pathom.connect.datascript
  (:require [datascript.core :as ds]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            [com.wsscode.pathom.sugar :as ps]
            [edn-query-language.core :as eql]))

(s/def ::db any?)
(s/def ::schema (s/map-of ::p/attribute map?))
(s/def ::schema-keys (s/coll-of ::p/attribute :kind set?))
(s/def ::schema-uniques ::schema-keys)
(s/def ::ident-attributes ::schema-keys)

(s/def ::schema-entry
  (s/keys
   :req []
   :opt [:db/doc :db/unique :db/cardinality :db/valueType]))

(s/def ::schema (s/map-of keyword? ::schema-entry))

(defn raw-datascript-db
  [conn]
  (ds/db conn))

(defn db->schema
  [db]
  (:schema db))

(defn schema->uniques
  "Return a set with the ident of the unique attributes in the schema."
  [schema]
  (->> schema
       (filter #(:db/unique (second %)))
       (into #{} (map first))))

(defn project-dependencies
  "Given a ::p/parent-query and ::schema-keys, compute all dependencies required to fulfil
  the ::p/parent-query."
  [{::keys    [schema-keys]
    ::p/keys  [parent-query]
    ::pc/keys [indexes sort-plan]
    :as       env}]
  (let [sort-plan   (or sort-plan pc/default-sort-plan)
        [good bad] (pc/split-good-bad-keys (p/entity env))
        non-datascript (keep
                        (fn [{:keys [key]}]
                          (if (contains? schema-keys key)
                            nil
                            key))
                        (:children (eql/query->shallow-ast parent-query)))]
    (into #{}
          (mapcat (fn [key]
                    (->> (pc/compute-paths*
                          (::pc/index-oir indexes)
                          good bad
                          key
                          #{key})
                         (sort-plan env)
                         first
                         (map first))))
          non-datascript)))

(defn ensure-minimum-subquery
  "Ensure the subquery has at least one element, this prevent empty sub-queries to be
  sent to datascript."
  [ast]
  (update ast :children
          (fn [items]
            (if (seq items)
              (mapv ensure-minimum-subquery items)
              [{:type :property :key :db/id :dispatch-key :db/id}]))))

(defn datascript-subquery
  "Given a ::p/parent-query, projects dependencies and compute the part of the query
  that Pathom delegates to datascript to fulfill."
  [{::p/keys [parent-query]
    ::keys   [schema-keys]
    :as      env}]
  (let [ent         (p/entity env)
        parent-keys (into #{} (map :key) (:children (eql/query->shallow-ast parent-query)))
        deps        (project-dependencies env)
        new-deps    (into []
                          (comp (filter schema-keys)
                                (map #(hash-map :key %)))
                          (set/difference deps parent-keys))]
    (->> parent-query
         (p/lift-placeholders env)
         p/query->ast
         (p/transduce-children
          (comp (filter (comp schema-keys :key))
                (map #(dissoc % :params))))
         ensure-minimum-subquery
         :children
         (into new-deps (comp (remove #(contains? ent (:key %))) ; remove already known keys
                                        ; remove ident attributes
                              (remove (comp vector? :key))))
         (hash-map :type :root :children)
         p/ast->query)))

(defn pick-ident-key
  "Figures which key to use to request data from datascript. This will
  try to pick :db/id if available, returning the number directly.
  Otherwise will look for some attribute that is a unique and is on
  the map, in case of multiple one will be selected by random. The
  format of the unique return is [:attribute value]."
  [{::keys [schema-uniques]} m]
  (if (contains? m :db/id)
    (:db/id m)

    (let [available (set/intersection schema-uniques (into #{} (keys m)))]
      (if (seq available)
        [(first available) (get m (first available))]))))

(defn datascript-resolve
  "Runs the resolver to fetch datascript data from identities."
  [{::keys [db]
    :as    config}
   env]
  (let [id       (pick-ident-key config (p/entity env))
        subquery (datascript-subquery (merge env config))]
    (println db id subquery)
    (ds/pull db subquery id)))

(defn index-schema
  "Creates Pathom index from datascript schema."
  [{::keys [schema] :as config}]
  (let [resolver  `datascript-resolver
        oir-paths {#{:db/id} #{resolver}}]
    {::pc/index-resolvers
     {resolver {::pc/sym            resolver
                ::pc/cache?         false
                ::pc/compute-output (fn [env]
                                      (datascript-subquery (merge env config)))
                ::datascript?       true
                ::pc/resolve        (fn [env _] (datascript-resolve config env))}}

     ::pc/index-oir
     (->> (reduce
           (fn [idx ident]
             (assoc idx ident oir-paths))
           {:db/id (zipmap (map hash-set (schema->uniques schema)) (repeat #{resolver}))}
           (keys schema)))}))

(def registry
  [(pc/single-attr-resolver ::conn ::db raw-datascript-db)
   (pc/single-attr-resolver ::db ::schema db->schema)
   (pc/single-attr-resolver ::schema ::schema-keys #(into #{:db/id} (keys %)))
   (pc/single-attr-resolver ::schema ::schema-uniques schema->uniques)])

(def config-parser (-> registry ps/connect-serial-parser ps/context-parser))

(defn normalize-config
  "Fulfill missing configuration options using inferences."
  [config]
  (config-parser config config
                 [::conn ::db ::schema ::schema-uniques ::schema-keys]))

(defn datascript-connect-plugin
  "Plugin to add datascript integration.

  Options:

  ::conn (required) - datascript connection
  ::db -  datascript db, if not provided will be computed from ::conn
  "
  [config]
  (let [config'  (normalize-config config)
        ds-index (index-schema config')]
    {::p/wrap-parser2
     (fn [parser {::p/keys [plugins]}]
       (let [idx-atoms (keep ::pc/indexes plugins)]
         (doseq [idx* idx-atoms]
           (swap! idx* pc/merge-indexes ds-index))
         (fn [env tx]
           (parser (merge env config') tx))))}))




;; (comment

;;   (pc/defresolver out-of-band-resolver
;;     [_ {:keys [other]}]
;;     {::pc/input #{:other}
;;      ::pc/output #{:out-of-band}}
;;     {:out-of-band (inc other)})

;;   (let [schema {:someId {:db/unique :db.unique/identity}
;;                 :other {}}
;;         conn   (ds/create-conn schema)]
;;     (ds/transact! conn [{:someId  1
;;                          :other 42}
;;                         {:someId  2
;;                          :other 100}])
;;     (def parser
;;       (p/parser
;;        {::p/env     {::p/reader               [p/map-reader
;;                                                pc/reader2
;;                                                pc/open-ident-reader
;;                                                p/env-placeholder-reader]
;;                      ::p/placeholder-prefixes #{">"}}
;;         ::p/mutate  pc/mutate
;;         ::p/plugins [(pc/connect-plugin {::pc/register [out-of-band-resolver]})
;;                      (datascript-connect-plugin {::conn conn})
;;                      p/error-handler-plugin
;;                      p/trace-plugin]}))
;;     (def conn conn)
;;     (index-schema {::schema schema}))


;;   (parser {} [{[:db/id 1] [:someId :other :not-found :out-of-band]}])

;;   )
