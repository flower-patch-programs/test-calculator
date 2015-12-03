(ns calculator.core
  (:gen-class)
  (:require [environ.core :refer [env]]
            [cheshire.core :refer :all]
            [langohr.core      :as rmq]
            [langohr.channel   :as lch]
            [langohr.queue     :as lq]
            [langohr.consumers :as lc]
            [langohr.basic     :as lb]
            [langohr.consumers :as lcons]))

(def queue-name "flowerpatch.calculator")
(def queue-expiration-time (* 1000 60 60 24)) ;; How long should the queue stick around if the consumer dies?

(def connection-config {:host     (env :rabbitmq-host)
                        :username (env :rabbitmq-username)
                        :password (env :rabbitmq-password)
                        :port     (Integer/parseInt (or (env :rabbitmq-port) "0"))
                        :vhost    (env :rabbitmq-vhost)})

;; (def connection-config {:host "127.0.0.1"
;;                         :username "guest"
;;                         :password "guest"
;;                         :port 5672
;;                         :vhost "/"})

(defn apply-operator [operator numbers]
  (case operator
    "*" (apply * numbers)
    "+" (apply + numbers)
    "-" (apply - numbers)
    "/" (apply / numbers)))

(defn handle-delivery
  "Handles message delivery"
  [ch {:keys [delivery-tag reply-to correlation-id]} payload]
  (let [msg-data (parse-string (String. payload "UTF-8") true)
        {:keys [operator numbers]} msg-data
        response (apply-operator operator numbers)]
    (println "Received message" msg-data)
    (lb/publish ch "" reply-to (str response) {:correlation-id correlation-id})
    (lb/ack ch delivery-tag)))

(defn -main
  [& args]
  (println connection-config)
  (let [conn  (rmq/connect connection-config)
        ch    (lch/open conn)
        qname queue-name]

    (println (format "[main] Connected. Channel id: %d" (.getChannelNumber ch)))
    (println "queue-name=" queue-name)
    
    ;; We don't want to delete our queue if we go offline, that will result in message loss.
    ;; Setting exclusive to true will also result in message loss if we go offline.
    ;; Some people recommend setting an expiration time instead, this means if we do anything
    ;; via an upgrade the old data will eventually be cleaned up but we have a window to come back
    ;; online.
    (lq/declare ch qname {:exclusive false
                          :auto-delete false
                          :arguments {"x-expires" queue-expiration-time}})
    
    (lb/qos ch 1) ;; Limit unacknowledged messages to 1
    (lcons/blocking-subscribe ch queue-name handle-delivery)
        
    ))


