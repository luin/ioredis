import { expect, use } from "chai";
import * as calculateKeySlot from "cluster-key-slot";

import { default as Cluster } from "../../../lib/cluster";
import MockServer from "../../helpers/mock_server";

use(require("chai-as-promised"));

/*
  In this suite, foo1 and foo5 are usually served by the same node in a 3-nodes cluster.
  Instead foo1 and foo2 are usually served by different nodes in a 3-nodes cluster.
*/
describe("autoPipelining for cluster", function () {
  beforeEach(() => {
    const slotTable = [
      [0, 5000, ["127.0.0.1", 30001]],
      [5001, 9999, ["127.0.0.1", 30002]],
      [10000, 16383, ["127.0.0.1", 30003]],
    ];

    new MockServer(30001, function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }

      if (argv[0] === "get" && argv[1] === "foo2") {
        return "bar2";
      }

      if (argv[0] === "get" && argv[1] === "foo6") {
        return "bar6";
      }

      if (argv[0] === "get" && argv[1] === "baz:foo10") {
        return "bar10";
      }
    });

    new MockServer(30002, function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }

      if (argv[0] === "get" && argv[1] === "foo3") {
        return "bar3";
      }

      if (argv[0] === "get" && argv[1] === "foo4") {
        return "bar4";
      }
    });

    new MockServer(30003, function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }

      if (argv[0] === "set" && !argv[2]) {
        return new Error("ERR wrong number of arguments for 'set' command");
      }

      if (argv[0] === "get" && argv[1] === "foo1") {
        return "bar1";
      }

      if (argv[0] === "get" && argv[1] === "foo5") {
        return "bar5";
      }

      if (argv[0] === "get" && argv[1] === "baz:foo1") {
        return "bar1";
      }

      if (argv[0] === "evalsha") {
        return argv.slice(argv.length - 4);
      }
    });
  });

  const hosts = [
    {
      host: "127.0.0.1",
      port: 30001,
    },
    {
      host: "127.0.0.1",
      port: 30002,
    },
    {
      host: "127.0.0.1",
      port: 30003,
    },
  ];

  it("should automatic add commands to auto pipelines", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await new Promise((resolve) => cluster.once("connect", resolve));

    await cluster.set("foo1", "bar1");
    expect(cluster.autoPipelineQueueSize).to.eql(0);

    const promise = cluster.get("foo1");
    expect(cluster.autoPipelineQueueSize).to.eql(1);

    const res = await promise;
    expect(res).to.eql("bar1");
    expect(cluster.autoPipelineQueueSize).to.eql(0);

    cluster.disconnect();
  });

  it("should not add non-compatible commands to auto pipelines", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await new Promise((resolve) => cluster.once("connect", resolve));

    expect(cluster.autoPipelineQueueSize).to.eql(0);
    const promises = [];

    promises.push(cluster.subscribe("subscribe").catch(() => {}));
    promises.push(cluster.unsubscribe("subscribe").catch(() => {}));

    expect(cluster.autoPipelineQueueSize).to.eql(0);
    await promises;

    cluster.disconnect();
  });

  it("should not add blacklisted commands to auto pipelines", async () => {
    const cluster = new Cluster(hosts, {
      enableAutoPipelining: true,
      autoPipeliningIgnoredCommands: ["hmget"],
    });
    await new Promise((resolve) => cluster.once("connect", resolve));

    expect(cluster.autoPipelineQueueSize).to.eql(0);

    const promise = cluster.hmget("foo1").catch(() => {});

    expect(cluster.autoPipelineQueueSize).to.eql(0);
    await promise;

    cluster.disconnect();
  });

  it("should support custom commands", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await new Promise((resolve) => cluster.once("connect", resolve));

    cluster.defineCommand("echo", {
      numberOfKeys: 2,
      lua: "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
    });

    const promise = cluster.echo("foo1", "foo1", "bar1", "bar2");
    expect(cluster.autoPipelineQueueSize).to.eql(1);
    expect(await promise).to.eql(["foo1", "foo1", "bar1", "bar2"]);

    await cluster.echo("foo1", "foo1", "bar1", "bar2");

    cluster.disconnect();
  });

  it("should support multiple commands", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await new Promise((resolve) => cluster.once("connect", resolve));

    await cluster.set("foo1", "bar1");
    await cluster.set("foo5", "bar5");

    expect(
      await Promise.all([
        cluster.get("foo1"),
        cluster.get("foo5"),
        cluster.get("foo1"),
        cluster.get("foo5"),
        cluster.get("foo1"),
      ])
    ).to.eql(["bar1", "bar5", "bar1", "bar5", "bar1"]);

    cluster.disconnect();
  });

  it("should support building pipelines when a prefix is used", async () => {
    const cluster = new Cluster(hosts, {
      enableAutoPipelining: true,
      keyPrefix: "baz:",
    });
    await new Promise((resolve) => cluster.once("connect", resolve));

    await cluster.set("foo1", "bar1");
    await cluster.set("foo10", "bar10");

    expect(
      await Promise.all([cluster.get("foo1"), cluster.get("foo10")])
    ).to.eql(["bar1", "bar10"]);

    cluster.disconnect();
  });

  it("should support commands queued after a pipeline is already queued for execution", (done) => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    cluster.once("connect", () => {
      let value1;
      expect(cluster.autoPipelineQueueSize).to.eql(0);

      cluster.set("foo1", "bar1", () => {});
      cluster.set("foo5", "bar5", () => {});

      cluster.get("foo1", (err, v1) => {
        expect(err).to.eql(null);
        value1 = v1;
      });

      process.nextTick(() => {
        cluster.get("foo5", (err, value2) => {
          expect(err).to.eql(null);

          expect(value1).to.eql("bar1");
          expect(value2).to.eql("bar5");
          expect(cluster.autoPipelineQueueSize).to.eql(0);

          cluster.disconnect();
          done();
        });
      });

      expect(cluster.autoPipelineQueueSize).to.eql(3);
    });
  });

  it("should correctly track pipeline length", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await new Promise((resolve) => cluster.once("connect", resolve));

    expect(cluster.autoPipelineQueueSize).to.eql(0);
    const promise1 = cluster.set("foo1", "bar");
    const promise2 = cluster.set("foo5", "bar");
    expect(cluster.autoPipelineQueueSize).to.eql(2);
    await promise1;
    await promise2;

    expect(cluster.autoPipelineQueueSize).to.eql(0);
    const promise3 = Promise.all([
      cluster.get("foo1"),
      cluster.get("foo5"),
      cluster.get("foo1"),
      cluster.get("foo5"),
      cluster.get("foo1"),
    ]);
    expect(cluster.autoPipelineQueueSize).to.eql(5);
    await promise3;

    cluster.disconnect();
  });

  it("should handle rejections", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await cluster.set("foo1", "bar");
    await expect(cluster.set("foo1")).to.eventually.be.rejectedWith(
      "ERR wrong number of arguments for 'set' command"
    );

    cluster.disconnect();
  });

  it("should support callbacks in the happy case", (done) => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    cluster.once("connect", () => {
      let value1, value2;

      function cb() {
        expect(value1).to.eql("bar1");
        expect(value2).to.eql("bar5");
        expect(cluster.autoPipelineQueueSize).to.eql(0);

        cluster.disconnect();
        done();
      }

      expect(cluster.autoPipelineQueueSize).to.eql(0);

      /*
        In this test, foo1 and foo5 usually (like in the case of 3 nodes scenario) belongs
        to different nodes group.
        Therefore we are also testing callback scenario with multiple pipelines fired together.
      */
      cluster.set("foo1", "bar1", () => {});

      expect(cluster.autoPipelineQueueSize).to.eql(1);

      cluster.set("foo5", "bar5", () => {
        cluster.get("foo1", (err, v1) => {
          expect(err).to.eql(null);
          value1 = v1;

          // This is needed as we cannot really predict which nodes responds first
          if (value1 && value2) {
            cb();
          }
        });

        expect(cluster.autoPipelineQueueSize).to.eql(1);

        cluster.get("foo5", (err, v2) => {
          expect(err).to.eql(null);
          value2 = v2;

          // This is needed as we cannot really predict which nodes responds first
          if (value1 && value2) {
            cb();
          }
        });

        expect(cluster.autoPipelineQueueSize).to.eql(2);
      });

      expect(cluster.autoPipelineQueueSize).to.eql(2);
    });
  });

  it("should support callbacks in the failure case", (done) => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    cluster.once("connect", () => {
      expect(cluster.autoPipelineQueueSize).to.eql(0);

      cluster.set("foo1", "bar1", (err) => {
        expect(err).to.eql(null);
      });

      expect(cluster.autoPipelineQueueSize).to.eql(1);

      cluster.set("foo5", (err) => {
        expect(err.message).to.eql(
          "ERR wrong number of arguments for 'set' command"
        );

        cluster.disconnect();
        done();
      });

      expect(cluster.autoPipelineQueueSize).to.eql(2);
    });
  });

  it("should handle callbacks failures", (done) => {
    const listeners = process.listeners("uncaughtException");
    process.removeAllListeners("uncaughtException");

    process.once("uncaughtException", (err) => {
      expect(err.message).to.eql("ERROR");

      for (const listener of listeners) {
        process.on("uncaughtException", listener);
      }

      cluster.disconnect();
      done();
    });

    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    cluster.once("connect", () => {
      expect(cluster.autoPipelineQueueSize).to.eql(0);

      cluster.set("foo1", "bar1", (err) => {
        expect(err).to.eql(null);

        throw new Error("ERROR");
      });

      cluster.set("foo5", "bar5", (err) => {
        expect(err).to.eql(null);

        expect(cluster.autoPipelineQueueSize).to.eql(0);
      });

      expect(cluster.autoPipelineQueueSize).to.eql(2);
    });
  });

  it("should handle general pipeline failures", (done) => {
    const listeners = process.listeners("uncaughtException");
    process.removeAllListeners("uncaughtException");

    process.once("uncaughtException", (err) => {
      expect(err.message).to.eql("ERROR");

      for (const listener of listeners) {
        process.on("uncaughtException", listener);
      }

      cluster.disconnect();
      done();
    });

    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    cluster.once("connect", () => {
      expect(cluster.autoPipelineQueueSize).to.eql(0);

      cluster.set("foo1", "bar1", (err) => {
        expect(err).to.eql(null);

        throw new Error("ERROR");
      });

      cluster.set("foo5", "bar5", (err) => {
        expect(err).to.eql(null);

        expect(cluster.autoPipelineQueueSize).to.eql(0);
      });

      expect(cluster.autoPipelineQueueSize).to.eql(2);
    });
  });

  it("should handle general pipeline rejections", async () => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });
    await new Promise((resolve) => cluster.once("connect", resolve));

    const promise1 = cluster.set("foo1", "bar");
    const promise2 = cluster.set("foo5", "bar");
    const promise3 = cluster.set("foo2", "bar");
    const promise4 = cluster.set("foo6", "bar");

    // Override slots to induce a failure
    const key1Slot = calculateKeySlot("foo1");
    const key2Slot = calculateKeySlot("foo2");
    const key5Slot = calculateKeySlot("foo5");
    cluster.slots[key1Slot] = cluster.slots[key2Slot];
    cluster.slots[key2Slot] = cluster.slots[key5Slot];

    await expect(promise1).to.eventually.be.rejectedWith(
      "All keys in the pipeline should belong to the same slots allocation group"
    );
    await expect(promise2).to.eventually.be.rejectedWith(
      "All keys in the pipeline should belong to the same slots allocation group"
    );
    await expect(promise3).to.eventually.be.rejectedWith(
      "All keys in the pipeline should belong to the same slots allocation group"
    );
    await expect(promise4).to.eventually.be.rejectedWith(
      "All keys in the pipeline should belong to the same slots allocation group"
    );

    cluster.disconnect();
  });

  it("should handle general pipeline failures", (done) => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    cluster.once("connect", () => {
      let err1, err2, err3, err4;

      function cb() {
        expect(err1.message).to.eql(
          "All keys in the pipeline should belong to the same slots allocation group"
        );
        expect(err2.message).to.eql(
          "All keys in the pipeline should belong to the same slots allocation group"
        );
        expect(err3.message).to.eql(
          "All keys in the pipeline should belong to the same slots allocation group"
        );
        expect(err4.message).to.eql(
          "All keys in the pipeline should belong to the same slots allocation group"
        );
        expect(cluster.autoPipelineQueueSize).to.eql(0);

        cluster.disconnect();
        done();
      }

      expect(cluster.autoPipelineQueueSize).to.eql(0);

      cluster.set("foo1", "bar1", (err) => {
        err1 = err;

        if (err1 && err2 && err3 && err4) {
          cb();
        }
      });

      expect(cluster.autoPipelineQueueSize).to.eql(1);

      cluster.set("foo2", "bar2", (err) => {
        err2 = err;

        if (err1 && err2 && err3 && err4) {
          cb();
        }
      });

      expect(cluster.autoPipelineQueueSize).to.eql(2);

      cluster.set("foo5", "bar5", (err) => {
        err3 = err;

        if (err1 && err2 && err3 && err4) {
          cb();
        }
      });

      expect(cluster.autoPipelineQueueSize).to.eql(3);

      cluster.set("foo6", "bar6", (err) => {
        err4 = err;

        if (err1 && err2 && err3 && err4) {
          cb();
        }
      });

      expect(cluster.autoPipelineQueueSize).to.eql(4);

      // Override slots to induce a failure
      const key1Slot = calculateKeySlot("foo1");
      const key2Slot = calculateKeySlot("foo2");
      const key5Slot = calculateKeySlot("foo5");
      cluster.slots[key1Slot] = cluster.slots[key2Slot];
      cluster.slots[key2Slot] = cluster.slots[key5Slot];
    });
  });

  it("should handle general pipeline failures callbacks failure", (done) => {
    const cluster = new Cluster(hosts, { enableAutoPipelining: true });

    const listeners = process.listeners("uncaughtException");
    process.removeAllListeners("uncaughtException");

    cluster.once("connect", () => {
      let err1, err5;

      process.once("uncaughtException", (err) => {
        expect(err.message).to.eql("ERROR");
        expect(err1.message).to.eql(
          "All keys in the pipeline should belong to the same slots allocation group"
        );
        expect(err5.message).to.eql(
          "All keys in the pipeline should belong to the same slots allocation group"
        );

        for (const listener of listeners) {
          process.on("uncaughtException", listener);
        }

        cluster.disconnect();
        done();
      });

      cluster.set("foo1", "bar1", (err) => {
        err1 = err;
      });
      cluster.set("foo5", "bar5", (err) => {
        err5 = err;
      });

      expect(cluster.autoPipelineQueueSize).to.eql(2);

      cluster.set("foo2", (err) => {
        throw new Error("ERROR");
      });

      expect(cluster.autoPipelineQueueSize).to.eql(3);

      const key1Slot = calculateKeySlot("foo1");
      const key2Slot = calculateKeySlot("foo2");
      cluster.slots[key1Slot] = cluster.slots[key2Slot];
    });
  });
});
