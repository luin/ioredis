import { createServer, Server, Socket } from "net";
import { EventEmitter } from "events";
import { convertBufferToString } from "../../lib/utils";
import enableDestroy = require("server-destroy");
import Parser = require("redis-parser");

let createdMockServers: MockServer[] = [];
const RAW_DATA_KEY = "___IOREDIS_MOCK_ROW_DATA___";

afterEach(function (done) {
  if (createdMockServers.length === 0) {
    done();
    return;
  }
  let pending = 0;
  for (const server of createdMockServers) {
    pending += 1;
    server.disconnect(check);
  }

  function check() {
    if (!--pending) {
      createdMockServers = [];
      done();
    }
  }
});

const connectionNameMap: WeakMap<Socket, string> = new WeakMap();

export function getConnectionName(socket: Socket): string | undefined {
  return connectionNameMap.get(socket);
}

interface IFlags {
  disconnect?: boolean;
}
export type MockServerHandler = (
  reply: any,
  socket: Socket,
  flags: IFlags
) => any;

export default class MockServer extends EventEmitter {
  static REDIS_OK = "+OK";

  static raw<T>(data: T): { [RAW_DATA_KEY]: T } {
    return {
      [RAW_DATA_KEY]: data,
    };
  }

  private clients: Socket[] = [];
  private socket?: Server;

  constructor(
    private port: number,
    public handler?: MockServerHandler,
    private slotTable?: any
  ) {
    super();
    this.connect();
    createdMockServers.push(this);
  }

  connect() {
    this.socket = createServer((c) => {
      const clientIndex = this.clients.push(c) - 1;
      process.nextTick(() => {
        this.emit("connect", c);
      });

      const parser = new Parser({
        returnBuffers: true,
        stringNumbers: false,
        returnReply: (reply: any) => {
          reply = convertBufferToString(reply, "utf8");
          if (
            reply.length === 3 &&
            reply[0].toLowerCase() === "client" &&
            reply[1].toLowerCase() === "setname"
          ) {
            connectionNameMap.set(c, reply[2]);
          }
          if (
            this.slotTable &&
            reply.length === 2 &&
            reply[0].toLowerCase() === "cluster" &&
            reply[1].toLowerCase() === "slots"
          ) {
            this.write(c, this.slotTable);
            return;
          }
          const flags: IFlags = {};
          const handlerResult = this.handler && this.handler(reply, c, flags);
          this.write(c, handlerResult);
          if (flags.disconnect) {
            this.disconnect();
          }
        },
        returnError: function () {},
      });

      c.on("end", () => {
        this.clients[clientIndex] = null;
        this.emit("disconnect", c);
      });

      c.on("data", (data) => {
        // @ts-ignore
        parser.execute(data);
      });
    });

    this.socket.listen(this.port);
    enableDestroy(this.socket);
  }

  disconnect(callback?: Function) {
    // @ts-ignore
    this.socket.destroy(callback);
  }

  broadcast(data: any) {
    this.clients
      .filter((c) => c)
      .forEach((client) => {
        this.write(client, data);
      });
  }

  write(c: Socket, data: any) {
    if (c.writable) {
      c.write(convert("", data));
    }

    function convert(str: string, data: any) {
      let result: string;
      if (typeof data === "undefined") {
        data = MockServer.REDIS_OK;
      }
      if (data === MockServer.REDIS_OK) {
        result = "+OK\r\n";
      } else if (data instanceof Error) {
        result = "-" + data.message + "\r\n";
      } else if (Array.isArray(data)) {
        result = "*" + data.length + "\r\n";
        data.forEach(function (item) {
          result += convert(str, item);
        });
      } else if (typeof data === "number") {
        result = ":" + data + "\r\n";
      } else if (data === null) {
        result = "$-1\r\n";
      } else if (typeof data === "object" && data[RAW_DATA_KEY]) {
        result = data[RAW_DATA_KEY];
      } else {
        data = data.toString();
        result = "$" + data.length + "\r\n";
        result += data + "\r\n";
      }
      return str + result;
    }
  }

  findClientByName(name: string): Socket | undefined {
    return this.clients
      .filter((c) => c)
      .find((client) => {
        return getConnectionName(client) === name;
      });
  }
}
