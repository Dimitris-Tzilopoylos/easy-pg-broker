export = Broker;
declare class Broker {
  static connectionString: string;
  static client: any;
  static connected: boolean;
  static enableLogger: boolean;
  static discoverIntervalMS: number;
  static discoverInterval: any;
  static clientId: any;
  static $discoverEventName: string;
  static $discoverReplyEventName: string;
  static brokerEvents: {};
  static cluster: any[];
  static autoDiscover: boolean;
  static endClient(): Promise<void>;
  static connectClient(url: any, reset?: boolean): Promise<void>;
  static listen(): void;
  static init({
    connectionString,
    discoverInterval,
    autoDiscover,
    clientId,
    logger,
  }: {
    connectionString: string;
    discoverInterval?: number;
    autoDiscover?: boolean;
    clientId?: any;
    logger?: boolean;
  }): Promise<void>;
  static subscribe(
    eventName: string,
    handler: (args: {
      data?: any;
      channel: string;
      processId: any;
      event: string;
      eventId: string;
      handlerId: string;
      handlerIndex: number;
      publisher: string | number;
      clientIds: any[];
      isPublisher: boolean;
      notificationId: string;
    }) => any
  ): Promise<void>;
  static notify(
    eventName: string,
    data?: any,
    clientIds?:
      | string
      | string[]
      | null
      | number
      | number[]
      | Array<string | number>
  ): Promise<void>;
  static logger(...args: any[]): void;
  static setClientId(id: string | number): void;
  static disableLogging(): void;
  static enableLogging(): void;
  static registerDiscoverEvents(): Promise<void>;
  static discover(): Promise<void>;
  static setDiscoverInterval(ms: number): void;
  static unsubscribe(eventName: string): Promise<void>;
}
