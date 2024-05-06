const { Client } = require("pg");
const { v4 } = require("uuid");

class Broker {
  static connectionString = "postgres://localhost:5432/postgres";
  static client = new Client();
  static connected = false;
  static enableLogger = true;
  static discoverIntervalMS = 6 * 1000;
  static discoverInterval;
  static clientId = v4();
  static $discoverEventName = "discover_clients";
  static $discoverReplyEventName = "discover_clients_reply";
  static brokerEvents = {};
  static cluster = [];
  static autoDiscover = false;
  static async endClient() {
    try {
      if (!this.connected) {
        return;
      }
      const result = await Promise.allSettled(
        Object.values(Broker.brokerEvents).map(({ unsubscribe }) =>
          unsubscribe()
        )
      );

      Broker.logger(result);
      await Broker.client.end();
      if (Broker.discoverInterval) {
        clearInterval(Broker.discoverInterval);
      }
      this.connected = false;
      Broker.logger("Disconnected");
    } catch (error) {
      Broker.logger(error);
    }
  }

  static async connectClient(url, reset = false) {
    try {
      if (reset) {
        await Broker.endClient();
        if (this.connected) {
          throw new Error();
        }
        Broker.client = new Client({ connectionString: url });
      }
      if (this.connected) {
        return;
      }
      await Broker.client.connect();
      this.connected = true;
      Broker.logger("Connection established");
      await Broker.registerDiscoverEvents();
      this.listen();
      if (Broker.discoverInterval) {
        clearInterval(Broker.discoverInterval);
      }
      if (Broker.autoDiscover) {
        Broker.cluster = [];
        Broker.discoverInterval = setInterval(() => {
          Broker.discover().catch((err) => {
            Broker.logger("Error in discover cluster");
          });
        }, Broker.discoverIntervalMS);
      }
    } catch (error) {
      Broker.logger(error);
    }
  }

  static listen() {
    Broker.client.on("notification", (msg) => {
      try {
        const { payload, channel, processId } = msg;

        const event = Broker.brokerEvents[channel];
        if (!event || !event.handlers.length) {
          return;
        }

        const { handlers } = event;
        const { data, clientIds, fromClientId, notificationId } =
          JSON.parse(payload);
        if (Array.isArray(clientIds) && clientIds.length) {
          const shouldBeNotified = clientIds.some(
            (clientId) => clientId === Broker.clientId
          );
          if (!shouldBeNotified) {
            Broker.logger(
              `Received notify from channel [${channel}] with ID [${event.id}] but skipping processing due to that broker is not an eligible recipient`
            );
            return;
          }
        }
        let i = 0;
        Broker.logger(
          `Received notify from channel [${channel}] with ID [${event.id}] having ${handlers.length} handlers`
        );
        const currentCliendId = Broker.clientId;
        for (const { handler, id } of handlers) {
          handler({
            data,
            channel,
            processId,
            event: channel,
            eventId: event.id,
            handlerId: id,
            handlerIndex: i++,
            publisher: currentCliendId,
            clientIds: clientIds || [],
            isPublisher: fromClientId === currentCliendId,
            notificationId,
          });
        }
      } catch (error) {
        Broker.logger(error);
      }
    });
  }

  static async init({
    connectionString,
    discoverInterval,
    autoDiscover = false,
    clientId,
    logger = false,
  }) {
    Broker.autoDiscover = autoDiscover;
    if (typeof discoverInterval === "number" && discoverInterval > 0) {
      Broker.setDiscoverInterval(discoverInterval);
    }

    Broker.setClientId(clientId || Broker.clientId);
    Broker.enableLogger = !!logger;
    await Broker.connectClient(connectionString, true);
  }

  static async subscribe(eventName, handler) {
    if (typeof handler !== "function") {
      throw new Error("handler should be a function");
    }
    let shouldListen = false;
    if (!Broker.brokerEvents[eventName]) {
      shouldListen = true;
      Broker.brokerEvents[eventName] = {
        handlers: [],
        id: v4(),
        unsubscribe: () => Broker.client.query(`UNLISTEN ${eventName}`),
        eventName,
      };
    }

    Broker.brokerEvents[eventName].handlers.push({ handler, id: v4() });

    if (shouldListen) {
      try {
        await Broker.client.query(`LISTEN ${eventName}`);
      } catch (error) {
        Broker.logger(error);
      }
    }
  }

  static async notify(eventName, data, clientIds = undefined) {
    try {
      if (Broker.connected && Broker.brokerEvents[eventName]) {
        const payload = JSON.stringify({
          data,
          fromClientId: Broker.clientId,
          notificationId: v4(),
          clientIds: !clientIds
            ? []
            : Array.isArray(clientIds)
            ? clientIds
            : [clientIds],
        });
        const sizeInBytes = Buffer.byteLength(payload, "utf8");
        if (sizeInBytes > 8000) {
          throw new Error("Notification payload is more than 8Kb");
        }
        await Broker.client.query(`NOTIFY ${eventName}, '${payload}'`);
      }
    } catch (error) {
      Broker.logger(error);
    }
  }

  static logger(...args) {
    if (Broker.enableLogger) {
      console.log(...args);
    }
  }

  static setClientId(id) {
    if (typeof id !== "string" && typeof id !== "number") {
      throw new Error(`clientId must be of type string or number`);
    }
    Broker.clientId = id;
  }

  static disableLogging() {
    Broker.enableLogger = false;
  }

  static enableLogging() {
    Broker.enableLogger = true;
  }

  static async registerDiscoverEvents() {
    await Broker.subscribe(
      Broker.$discoverEventName,
      ({ isPublisher, publisher }) => {
        if (isPublisher) {
          Broker.logger(
            `Skipping [${Broker.$discoverEventName}] due to being the publisher in this transaction`
          );
          return;
        }

        Broker.notify(
          Broker.$discoverReplyEventName,
          Object.values(Broker.brokerEvents)
            .map((value) => ({
              eventName: value.eventName,
              id: value.id,
            }))
            .filter(
              (x) =>
                [
                  Broker.$discoverReplyEventName,
                  Broker.$discoverEventName,
                ].indexOf(x.eventName) === -1
            ),
          publisher
        ).catch((err) => {
          Broker.logger(err);
        });
      }
    );

    await Broker.subscribe(
      Broker.$discoverReplyEventName,
      ({ publisher, data }) => {
        if (publisher !== Broker.clientId) {
          Broker.logger(
            `Skipping [${Broker.$discoverReplyEventName}] due to not being the publisher in this transaction`
          );
          return;
        }
        Broker.cluster = [...new Set(Broker.cluster.concat(publisher))].filter(
          Boolean
        );
      }
    );
  }

  static async discover() {
    try {
      await Broker.notify(Broker.$discoverEventName, Broker.clientId);
    } catch (error) {}
  }

  static setDiscoverInterval(ms) {
    if (Broker.discoverInterval) {
      clearInterval(Broker.discoverInterval);
    }

    if (typeof ms !== "number" || ms <= 0) {
      throw new Error("ms should be a positive number");
    }
    Broker.discoverIntervalMS = ms;
    if (Broker.autoDiscover) {
      Broker.cluster = [];
      Broker.discoverInterval = setInterval(() => {
        Broker.discover().catch((err) => {
          Broker.logger("Error in discover cluster");
        });
      }, Broker.discoverIntervalMS);
    }
  }

  static async unsubscribe(eventName) {
    try {
      if (!Broker.brokerEvents[eventName]) {
        return;
      }

      const { unsubscribe } = Broker.brokerEvents[eventName];
      await unsubscribe();
      delete Broker.brokerEvents[eventName];
    } catch (error) {}
  }
}

module.exports = Broker;
