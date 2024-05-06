var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
var Client = require("pg").Client;
var v4 = require("uuid").v4;
var Broker = /** @class */ (function () {
    function Broker() {
    }
    Broker.endClient = function () {
        return __awaiter(this, void 0, void 0, function () {
            var result, error_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 3, , 4]);
                        if (!this.connected) {
                            return [2 /*return*/];
                        }
                        return [4 /*yield*/, Promise.allSettled(Object.values(Broker.brokerEvents).map(function (_a) {
                                var unsubscribe = _a.unsubscribe;
                                return unsubscribe();
                            }))];
                    case 1:
                        result = _a.sent();
                        Broker.logger(result);
                        return [4 /*yield*/, Broker.client.end()];
                    case 2:
                        _a.sent();
                        if (Broker.discoverInterval) {
                            clearInterval(Broker.discoverInterval);
                        }
                        this.connected = false;
                        Broker.logger("Disconnected");
                        return [3 /*break*/, 4];
                    case 3:
                        error_1 = _a.sent();
                        Broker.logger(error_1);
                        return [3 /*break*/, 4];
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    Broker.connectClient = function (url_1) {
        return __awaiter(this, arguments, void 0, function (url, reset) {
            var error_2;
            if (reset === void 0) { reset = false; }
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 5, , 6]);
                        if (!reset) return [3 /*break*/, 2];
                        return [4 /*yield*/, Broker.endClient()];
                    case 1:
                        _a.sent();
                        if (this.connected) {
                            throw new Error();
                        }
                        Broker.client = new Client({ connectionString: url });
                        _a.label = 2;
                    case 2:
                        if (this.connected) {
                            return [2 /*return*/];
                        }
                        return [4 /*yield*/, Broker.client.connect()];
                    case 3:
                        _a.sent();
                        this.connected = true;
                        Broker.logger("Connection established");
                        return [4 /*yield*/, Broker.registerDiscoverEvents()];
                    case 4:
                        _a.sent();
                        this.listen();
                        if (Broker.discoverInterval) {
                            clearInterval(Broker.discoverInterval);
                        }
                        if (Broker.autoDiscover) {
                            Broker.cluster = [];
                            Broker.discoverInterval = setInterval(function () {
                                Broker.discover().catch(function (err) {
                                    Broker.logger("Error in discover cluster");
                                });
                            }, Broker.discoverIntervalMS);
                        }
                        return [3 /*break*/, 6];
                    case 5:
                        error_2 = _a.sent();
                        Broker.logger(error_2);
                        return [3 /*break*/, 6];
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    Broker.listen = function () {
        Broker.client.on("notification", function (msg) {
            try {
                var payload = msg.payload, channel = msg.channel, processId = msg.processId;
                var event_1 = Broker.brokerEvents[channel];
                if (!event_1 || !event_1.handlers.length) {
                    return;
                }
                var handlers = event_1.handlers;
                var _a = JSON.parse(payload), data = _a.data, clientIds = _a.clientIds, fromClientId = _a.fromClientId, notificationId = _a.notificationId;
                if (Array.isArray(clientIds) && clientIds.length) {
                    var shouldBeNotified = clientIds.some(function (clientId) { return clientId === Broker.clientId; });
                    if (!shouldBeNotified) {
                        Broker.logger("Received notify from channel [".concat(channel, "] with ID [").concat(event_1.id, "] but skipping processing due to that broker is not an eligible recipient"));
                        return;
                    }
                }
                var i = 0;
                Broker.logger("Received notify from channel [".concat(channel, "] with ID [").concat(event_1.id, "] having ").concat(handlers.length, " handlers"));
                var currentCliendId = Broker.clientId;
                for (var _i = 0, handlers_1 = handlers; _i < handlers_1.length; _i++) {
                    var _b = handlers_1[_i], handler = _b.handler, id = _b.id;
                    handler({
                        data: data,
                        channel: channel,
                        processId: processId,
                        event: channel,
                        eventId: event_1.id,
                        handlerId: id,
                        handlerIndex: i++,
                        publisher: currentCliendId,
                        clientIds: clientIds || [],
                        isPublisher: fromClientId === currentCliendId,
                        notificationId: notificationId,
                    });
                }
            }
            catch (error) {
                Broker.logger(error);
            }
        });
    };
    Broker.init = function (_a) {
        return __awaiter(this, arguments, void 0, function (_b) {
            var connectionString = _b.connectionString, discoverInterval = _b.discoverInterval, _c = _b.autoDiscover, autoDiscover = _c === void 0 ? false : _c, clientId = _b.clientId, _d = _b.logger, logger = _d === void 0 ? false : _d;
            return __generator(this, function (_e) {
                switch (_e.label) {
                    case 0:
                        Broker.autoDiscover = autoDiscover;
                        if (typeof discoverInterval === "number" && discoverInterval > 0) {
                            Broker.setDiscoverInterval(discoverInterval);
                        }
                        Broker.setClientId(clientId || Broker.clientId);
                        Broker.enableLogger = !!logger;
                        return [4 /*yield*/, Broker.connectClient(connectionString, true)];
                    case 1:
                        _e.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    Broker.subscribe = function (eventName, handler) {
        return __awaiter(this, void 0, void 0, function () {
            var shouldListen, error_3;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (typeof handler !== "function") {
                            throw new Error("handler should be a function");
                        }
                        shouldListen = false;
                        if (!Broker.brokerEvents[eventName]) {
                            shouldListen = true;
                            Broker.brokerEvents[eventName] = {
                                handlers: [],
                                id: v4(),
                                unsubscribe: function () { return Broker.client.query("UNLISTEN ".concat(eventName)); },
                                eventName: eventName,
                            };
                        }
                        Broker.brokerEvents[eventName].handlers.push({ handler: handler, id: v4() });
                        if (!shouldListen) return [3 /*break*/, 4];
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, Broker.client.query("LISTEN ".concat(eventName))];
                    case 2:
                        _a.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        error_3 = _a.sent();
                        Broker.logger(error_3);
                        return [3 /*break*/, 4];
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    Broker.notify = function (eventName_1, data_1) {
        return __awaiter(this, arguments, void 0, function (eventName, data, clientIds) {
            var payload, sizeInBytes, error_4;
            if (clientIds === void 0) { clientIds = undefined; }
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 3, , 4]);
                        if (!(Broker.connected && Broker.brokerEvents[eventName])) return [3 /*break*/, 2];
                        payload = JSON.stringify({
                            data: data,
                            fromClientId: Broker.clientId,
                            notificationId: v4(),
                            clientIds: !clientIds
                                ? []
                                : Array.isArray(clientIds)
                                    ? clientIds
                                    : [clientIds],
                        });
                        sizeInBytes = Buffer.byteLength(payload, "utf8");
                        if (sizeInBytes > 8000) {
                            throw new Error("Notification payload is more than 8Kb");
                        }
                        return [4 /*yield*/, Broker.client.query("NOTIFY ".concat(eventName, ", '").concat(payload, "'"))];
                    case 1:
                        _a.sent();
                        _a.label = 2;
                    case 2: return [3 /*break*/, 4];
                    case 3:
                        error_4 = _a.sent();
                        Broker.logger(error_4);
                        return [3 /*break*/, 4];
                    case 4: return [2 /*return*/];
                }
            });
        });
    };
    Broker.logger = function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (Broker.enableLogger) {
            console.log.apply(console, args);
        }
    };
    Broker.setClientId = function (id) {
        if (typeof id !== "string" && typeof id !== "number") {
            throw new Error("clientId must be of type string or number");
        }
        Broker.clientId = id;
    };
    Broker.disableLogging = function () {
        Broker.enableLogger = false;
    };
    Broker.enableLogging = function () {
        Broker.enableLogger = true;
    };
    Broker.registerDiscoverEvents = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, Broker.subscribe(Broker.$discoverEventName, function (_a) {
                            var isPublisher = _a.isPublisher, publisher = _a.publisher;
                            if (isPublisher) {
                                Broker.logger("Skipping [".concat(Broker.$discoverEventName, "] due to being the publisher in this transaction"));
                                return;
                            }
                            Broker.notify(Broker.$discoverReplyEventName, Object.values(Broker.brokerEvents)
                                .map(function (value) { return ({
                                eventName: value.eventName,
                                id: value.id,
                            }); })
                                .filter(function (x) {
                                return [
                                    Broker.$discoverReplyEventName,
                                    Broker.$discoverEventName,
                                ].indexOf(x.eventName) === -1;
                            }), publisher).catch(function (err) {
                                Broker.logger(err);
                            });
                        })];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, Broker.subscribe(Broker.$discoverReplyEventName, function (_a) {
                                var publisher = _a.publisher, data = _a.data;
                                if (publisher !== Broker.clientId) {
                                    Broker.logger("Skipping [".concat(Broker.$discoverReplyEventName, "] due to not being the publisher in this transaction"));
                                    return;
                                }
                                Broker.cluster = __spreadArray([], new Set(Broker.cluster.concat(publisher)), true).filter(Boolean);
                            })];
                    case 2:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    Broker.discover = function () {
        return __awaiter(this, void 0, void 0, function () {
            var error_5;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, Broker.notify(Broker.$discoverEventName, Broker.clientId)];
                    case 1:
                        _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        error_5 = _a.sent();
                        return [3 /*break*/, 3];
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    Broker.setDiscoverInterval = function (ms) {
        if (Broker.discoverInterval) {
            clearInterval(Broker.discoverInterval);
        }
        if (typeof ms !== "number" || ms <= 0) {
            throw new Error("ms should be a positive number");
        }
        Broker.discoverIntervalMS = ms;
        if (Broker.autoDiscover) {
            Broker.cluster = [];
            Broker.discoverInterval = setInterval(function () {
                Broker.discover().catch(function (err) {
                    Broker.logger("Error in discover cluster");
                });
            }, Broker.discoverIntervalMS);
        }
    };
    Broker.unsubscribe = function (eventName) {
        return __awaiter(this, void 0, void 0, function () {
            var unsubscribe, error_6;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        if (!Broker.brokerEvents[eventName]) {
                            return [2 /*return*/];
                        }
                        unsubscribe = Broker.brokerEvents[eventName].unsubscribe;
                        return [4 /*yield*/, unsubscribe()];
                    case 1:
                        _a.sent();
                        delete Broker.brokerEvents[eventName];
                        return [3 /*break*/, 3];
                    case 2:
                        error_6 = _a.sent();
                        return [3 /*break*/, 3];
                    case 3: return [2 /*return*/];
                }
            });
        });
    };
    Broker.connectionString = "postgres://localhost:5432/postgres";
    Broker.client = new Client();
    Broker.connected = false;
    Broker.enableLogger = true;
    Broker.discoverIntervalMS = 6 * 1000;
    Broker.clientId = v4();
    Broker.$discoverEventName = "discover_clients";
    Broker.$discoverReplyEventName = "discover_clients_reply";
    Broker.brokerEvents = {};
    Broker.cluster = [];
    Broker.autoDiscover = false;
    return Broker;
}());
module.exports = Broker;
