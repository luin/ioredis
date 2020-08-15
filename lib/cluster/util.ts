import { parseURL } from "../utils";
import { isIP } from "net";

export type NodeKey = string;
export type NodeRole = "master" | "slave" | "all";

export interface IRedisOptions {
  port: number;
  host: string;
  username?: string;
  password?: string;
  [key: string]: any;
}

export function getNodeKey(node: IRedisOptions): NodeKey {
  node.port = node.port || 6379;
  node.host = node.host || "127.0.0.1";
  return node.host + ":" + node.port;
}

export function nodeKeyToRedisOptions(nodeKey: NodeKey): IRedisOptions {
  const portIndex = nodeKey.lastIndexOf(":");
  if (portIndex === -1) {
    throw new Error(`Invalid node key ${nodeKey}`);
  }
  return {
    host: nodeKey.slice(0, portIndex),
    port: Number(nodeKey.slice(portIndex + 1)),
  };
}

export function normalizeNodeOptions(
  nodes: Array<string | number | object>
): IRedisOptions[] {
  return nodes.map((node) => {
    const options: any = {};
    if (typeof node === "object") {
      Object.assign(options, node);
    } else if (typeof node === "string") {
      Object.assign(options, parseURL(node));
    } else if (typeof node === "number") {
      options.port = node;
    } else {
      throw new Error("Invalid argument " + node);
    }
    if (typeof options.port === "string") {
      options.port = parseInt(options.port, 10);
    }

    // Cluster mode only support db 0
    delete options.db;

    if (!options.port) {
      options.port = 6379;
    }
    if (!options.host) {
      options.host = "127.0.0.1";
    } else {
      options.hostOriginal = options.host;
    }

    return options;
  });
}

export function getUniqueHostnamesFromOptions(
  nodes: IRedisOptions[]
): string[] {
  const uniqueHostsMap = {};
  nodes.forEach((node) => {
    uniqueHostsMap[node.host] = true;
  });

  return Object.keys(uniqueHostsMap).filter((host) => !isIP(host));
}
