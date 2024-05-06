# easy-pg-broker

`easy-pg-broker` provides a simple and easy-to-use interface for managing PostgreSQL's LISTEN/NOTIFY feature.

## Installation

You can install `easy-pg-broker` via npm:

```bash
npm install easy-pg-broker
```

## Usage

### Initialization

First, import `easy-pg-broker` and initialize the broker:

```javascript
const Broker = require("easy-pg-broker");

const run = async () => {
  await Broker.init({
    connectionString: "postgres://postgres:postgres@localhost:5435/postgres",
    clientId: "app",
    logger: true, // Set to true to enable logging
  });

  // Subscriptions and notifications go here
};

run();
```

### Subscriptions

Subscribe to specific channels to receive notifications:

```javascript
await Broker.subscribe("test1", async (args) => {
  // Handle the notification
});
```

### Notifications

Send notifications to specific channels:

```javascript
await Broker.notify("test1", Math.random());
```

### Example

Here's an example of how you can use `easy-pg-broker`:

```javascript
const Broker = require("easy-pg-broker");

const run = async () => {
  await Broker.init({
    connectionString: "postgres://postgres:postgres@localhost:5435/postgres",
    clientId: "app",
    logger: true,
  });

  await Broker.subscribe("test1", async (args) => {
    // Handle the notification
    console.log(args);
  });

  await Broker.subscribe("test2", (args) => {
    console.log(args);
  });

  await Broker.subscribe("test1", (args) => {});

  setInterval(async () => {
    await Broker.notify("test1", Math.random()); // will be handled by all listeners on channel test 1
    await Broker.notify("test2", { message: "x" }, "app"); // will be handled only by listeners on channel test2 having clientId set to app
    await Broker.notify("test2", {}, ["app", "abc"]); // will be handled only by abc and app listeners of channel test2
  }, 2000);
};

run();
```

## API

### `init(options)`

Initializes the broker with the provided options.

- `options`: An object containing the following properties:
  - `connectionString`: The PostgreSQL connection string.
  - `clientId`: An optional client identifier.
  - `logger`: Boolean flag indicating whether to enable logging.

### `subscribe(channel, callback)`

Subscribes to a channel to receive notifications.

- `channel`: The name of the channel to subscribe to.
- `callback`: The callback function to execute when a notification is received. It takes one argument, `args`, which contains the notification payload as well as some metadata.

### `notify(channel, payload, [clientIds])`

Sends a notification to a channel.

- `channel`: The name of the channel to send the notification to.
- `payload`: The payload to send with the notification.
- `clientIds`: An optional client identifier (can be undefined, number, string or an array containing strings or/and numbers). If specified, the notification will be handled only by clients with the specified IDs that are already listening to the channel using this package.

## Notice

Please note that PostgreSQL's LISTEN/NOTIFY feature has a maximum payload size limitation of 8KB (including metadata sent by this library). If the size of the payload exceeds this limit, an error will be thrown. Ensure that your payloads, including any metadata, are within this size limit to avoid unexpected errors.
