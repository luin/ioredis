import * as calculateSlot from "cluster-key-slot";
import MockServer from "../../helpers/mock_server";
import { expect } from "chai";
import { Cluster } from "../../../lib";
import * as sinon from "sinon";

describe("NAT", () => {
  it("works for normal case", (done) => {
    const slotTable = [
      [0, 1, ["192.168.1.1", 30001]],
      [2, 16383, ["192.168.1.2", 30001]],
    ];

    let cluster;
    new MockServer(30001, null, slotTable);
    new MockServer(
      30002,
      ([command, arg]) => {
        if (command === "get" && arg === "foo") {
          cluster.disconnect();
          done();
        }
      },
      slotTable
    );

    cluster = new Cluster(
      [
        {
          host: "127.0.0.1",
          port: 30001,
        },
      ],
      {
        natMap: {
          "192.168.1.1:30001": { host: "127.0.0.1", port: 30001 },
          "192.168.1.2:30001": { host: "127.0.0.1", port: 30002 },
        },
      }
    );

    cluster.get("foo");
  });

  it("works if natMap does not match all the cases", (done) => {
    const slotTable = [
      [0, 1, ["192.168.1.1", 30001]],
      [2, 16383, ["127.0.0.1", 30002]],
    ];

    let cluster;
    new MockServer(30001, null, slotTable);
    new MockServer(
      30002,
      ([command, arg]) => {
        if (command === "get" && arg === "foo") {
          cluster.disconnect();
          done();
        }
      },
      slotTable
    );

    cluster = new Cluster(
      [
        {
          host: "127.0.0.1",
          port: 30001,
        },
      ],
      {
        natMap: {
          "192.168.1.1:30001": { host: "127.0.0.1", port: 30001 },
        },
      }
    );

    cluster.get("foo");
  });

  it("works for moved", (done) => {
    const slotTable = [[0, 16383, ["192.168.1.1", 30001]]];

    let cluster;
    new MockServer(
      30001,
      ([command, arg]) => {
        if (command === "get" && arg === "foo") {
          return new Error(
            "MOVED " + calculateSlot("foo") + " 192.168.1.2:30001"
          );
        }
      },
      slotTable
    );
    new MockServer(
      30002,
      ([command, arg]) => {
        if (command === "get" && arg === "foo") {
          cluster.disconnect();
          done();
        }
      },
      slotTable
    );

    cluster = new Cluster(
      [
        {
          host: "127.0.0.1",
          port: 30001,
        },
      ],
      {
        natMap: {
          "192.168.1.1:30001": { host: "127.0.0.1", port: 30001 },
          "192.168.1.2:30001": { host: "127.0.0.1", port: 30002 },
        },
      }
    );

    cluster.get("foo");
  });

  it("works for ask", (done) => {
    const slotTable = [[0, 16383, ["192.168.1.1", 30001]]];

    let cluster;
    let asked = false;
    new MockServer(
      30001,
      ([command, arg]) => {
        if (command === "get" && arg === "foo") {
          return new Error(
            "ASK " + calculateSlot("foo") + " 192.168.1.2:30001"
          );
        }
      },
      slotTable
    );
    new MockServer(
      30002,
      ([command, arg]) => {
        if (command === "asking") {
          asked = true;
        }
        if (command === "get" && arg === "foo") {
          if (!asked) {
            throw new Error("expected asked to be true");
          }
          cluster.disconnect();
          done();
        }
      },
      slotTable
    );

    cluster = new Cluster(
      [
        {
          host: "127.0.0.1",
          port: 30001,
        },
      ],
      {
        natMap: {
          "192.168.1.1:30001": { host: "127.0.0.1", port: 30001 },
          "192.168.1.2:30001": { host: "127.0.0.1", port: 30002 },
        },
      }
    );

    cluster.get("foo");
  });

  it("keeps options immutable", (done) => {
    const slotTable = [[0, 16383, ["192.168.1.1", 30001]]];

    new MockServer(30001, null, slotTable);

    const cluster = new Cluster(
      [
        {
          host: "127.0.0.1",
          port: 30001,
        },
      ],
      Object.freeze({
        natMap: Object.freeze({
          "192.168.1.1:30001": Object.freeze({
            host: "127.0.0.1",
            port: 30001,
          }),
        }),
      })
    );

    const reset = sinon.spy(cluster.connectionPool, "reset");

    cluster.on("ready", () => {
      expect(reset.secondCall.args[0]).to.deep.equal([
        {
          host: "127.0.0.1",
          hostOriginal: "127.0.0.1",
          port: 30001,
          readOnly: false,
        },
      ]);
      cluster.disconnect();
      done();
    });
  });
});
