(ns cjsauer.pathom.connect.datascript-test
  (:require [cjsauer.pathom.connect.datascript :as pcd]
            [datascript.core :as ds]
            [com.wsscode.pathom.core :as p]
            [com.wsscode.pathom.connect :as pc]
            #?(:clj [clojure.test :as t :refer [deftest is testing]]
               :cljs [cljs.test :as t :refer [deftest is testing]])))

(def schema {:artist/gid  {:db/unique :db.unique/identity}
             :artist/name {}})

(def conn (ds/create-conn schema))

(def config {::pcd/conn conn})

(deftest sanity
  (is (= 2 (inc 1))))

(deftest test-schema->uniques
  (is (= (pcd/schema->uniques schema)
         #{:artist/gid})))

(deftest test-datascript-subquery
  (testing "basic sub query computing"
    (is (= (pcd/datascript-subquery {::p/parent-query  [:foo :bar :baz]
                                     ::pcd/schema-keys #{:foo :baz}})
           [:foo :baz])))

  (testing "halt on missing joins"
    (is (= (pcd/datascript-subquery {::p/parent-query  [:foo {:bar [:baz]}]
                                     ::pcd/schema-keys #{:foo :baz}})
           [:foo])))

  (testing "ensure minimal sub query"
    (is (= (pcd/datascript-subquery {::p/parent-query  [:foo {:baz [:bar]}]
                                     ::pcd/schema-keys #{:foo :baz}})
           [:foo {:baz [:db/id]}]))))

(deftest test-pick-ident-key
  (let [config (pcd/config-parser config {::pcd/conn conn} [::pcd/schema-uniques])]
    (testing "nothing available"
      (is (= (pcd/pick-ident-key config
                                 {})
             nil))
      (is (= (pcd/pick-ident-key config
                                 {:id  123
                                  :foo "bar"})
             nil)))
    (testing "pick from :db/id"
      (is (= (pcd/pick-ident-key config
                                 {:db/id 123})
             123)))
    (testing "picking from schema unique"
      (is (= (pcd/pick-ident-key config
                                 {:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"})
             [:artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"])))
    (testing "prefer :db/id"
      (is (= (pcd/pick-ident-key config
                                 {:db/id      123
                                  :artist/gid #uuid"76c9a186-75bd-436a-85c0-823e3efddb7f"})
             123)))))

(def index-schema-output
  `{:artist/gid  {#{:db/id} #{pcd/datascript-resolver}}
    :artist/name {#{:db/id} #{pcd/datascript-resolver}}
    :db/id       {#{:artist/gid} #{pcd/datascript-resolver}}
    })

(deftest test-index-schema
  (let [index (pcd/index-schema {::pcd/schema schema})]
    (is (= (::pc/index-oir index)
           index-schema-output))))

(deftest test-ensure-minimum-subquery
  (is (= (-> (p/query->ast [])
             (pcd/ensure-minimum-subquery)
             (p/ast->query))
         [:db/id]))
  (is (= (-> (p/query->ast [{:foo []}])
             (pcd/ensure-minimum-subquery)
             (p/ast->query))
         [{:foo [:db/id]}])))

(pc/defresolver out-of-band-resolver
  [_ _]
  {::pc/output #{:out-of-band}}
  {:out-of-band "good"})

(pc/defresolver greeting
  [_ {artist-name :artist/name}]
  {::pc/input #{:artist/name}
   ::pc/output #{:artist/greeting}}
  {:artist/greeting (str "Hello, " artist-name)})

(pc/defresolver query-resolver
  [env _]
  {::pc/output #{:artists-starting-with-F}}
  {:artists-starting-with-F
   (pcd/query-entities env '{:where
                             [[?e :artist/name ?name]
                              [(= "F" (first ?name))]]})})

(deftest test-datascript-parser
  (let [conn   (ds/create-conn schema)
        _      (ds/transact! conn [{:artist/gid  100
                                    :artist/name "Freddie"}])
        parser (p/parser
                {::p/env     {::p/reader               [p/map-reader
                                                        pc/reader2
                                                        pc/open-ident-reader
                                                        p/env-placeholder-reader]
                              ::p/placeholder-prefixes #{">"}}
                 ::p/mutate  pc/mutate
                 ::p/plugins [(pc/connect-plugin {::pc/register [out-of-band-resolver
                                                                 greeting
                                                                 query-resolver]})
                              (pcd/datascript-connect-plugin {::pcd/conn conn})
                              p/error-handler-plugin
                              p/trace-plugin]})]

    (testing "datascript keys not directly queried are automatically sourced when depended upon"
      (is (= (parser {} [{[:db/id 1] [:artist/gid :artist/greeting :out-of-band]}])
             {[:db/id 1] {:artist/gid      100
                          :artist/greeting "Hello, Freddie"
                          :out-of-band     "good"
                          }})))

    (testing "failed parses"
      (is (= (parser {} [{[:db/id 999999] [:does-not-exist]}])
             {[:db/id 999999] {:does-not-exist ::p/not-found}})))

    (testing "custom queries in resolvers"
      (is (= (parser {} [:artists-starting-with-F])
             {:artists-starting-with-F [[:db/id 1]]})))

    (testing "new transactions are picked up"
      (is (do
            (ds/transact! conn [{:artist/gid  200
                                 :artist/name "Bono"}])
            (= (parser {} [{[:artist/gid 200] [:artist/gid :artist/greeting :out-of-band]}])
               {[:artist/gid 200] {:artist/gid      200
                                   :artist/greeting "Hello, Bono"
                                   :out-of-band     "good"
                                   }}))))))
