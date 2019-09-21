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

(deftest test-db->schema
  (is (= (pcd/db->schema (ds/db conn))
         schema)))

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

(pc/defresolver derived-resolver
  [_ {:artist/keys [name]}]
  {::pc/input #{:artist/name}
   ::pc/output #{:derived}}
  {:derived (str name " Mercury")})

(ds/transact! conn [{:artist/gid 100
                     :artist/name "Freddie"}
                    {:artist/gid 200
                     :artist/name "Bono"}])

(def parser
  (p/parser
   {::p/env     {::p/reader               [p/map-reader
                                           pc/reader2
                                           pc/open-ident-reader
                                           p/env-placeholder-reader]
                 ::p/placeholder-prefixes #{">"}}
    ::p/mutate  pc/mutate
    ::p/plugins [(pc/connect-plugin {::pc/register [out-of-band-resolver
                                                    derived-resolver]})
                 (pcd/datascript-connect-plugin {::pcd/conn conn})
                 p/error-handler-plugin
                 p/trace-plugin]}))

(deftest test-datascript-parser
  (is (= (parser {} [{[:db/id 1] [:artist/gid :derived :out-of-band :not-found]}])
         {[:db/id 1] {:artist/gid  100
                      :derived     "Freddie Mercury"
                      :out-of-band "good"
                      :not-found   ::p/not-found
                      }})))

