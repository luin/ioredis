import Redis from "../../lib/redis";
import { expect } from "chai";
import { Readable } from "stream";
import * as sinon from "sinon";
import MockServer from "../helpers/mock_server";
import { Cluster } from "../../lib";

describe("*scanStream", function () {
  describe("scanStream", function () {
    it("should return a readable stream", function () {
      const redis = new Redis();
      const stream = redis.scanStream();
      expect(stream instanceof Readable).to.eql(true);
    });

    it("should iterate all keys", function (done) {
      let keys = [];
      const redis = new Redis();
      redis.mset(
        "foo1",
        1,
        "foo2",
        1,
        "foo3",
        1,
        "foo4",
        1,
        "foo10",
        1,
        function () {
          const stream = redis.scanStream();
          stream.on("data", function (data) {
            keys = keys.concat(data);
          });
          stream.on("end", function () {
            expect(keys.sort()).to.eql([
              "foo1",
              "foo10",
              "foo2",
              "foo3",
              "foo4",
            ]);
            redis.disconnect();
            done();
          });
        }
      );
    });

    it("should recognize `MATCH`", function (done) {
      let keys = [];
      const redis = new Redis();
      redis.mset(
        "foo1",
        1,
        "foo2",
        1,
        "foo3",
        1,
        "foo4",
        1,
        "foo10",
        1,
        function () {
          const stream = redis.scanStream({
            match: "foo??",
          });
          stream.on("data", function (data) {
            keys = keys.concat(data);
          });
          stream.on("end", function () {
            expect(keys).to.eql(["foo10"]);
            redis.disconnect();
            done();
          });
        }
      );
    });

    it("should recognize `TYPE`", function (done) {
      let keys = [];
      const redis = new Redis();
      redis.set("foo1", "bar");
      redis.set("foo2", "bar");
      redis.set("foo3", "bar");
      redis.lpush("loo1", "1");
      redis.lpush("loo2", "1");
      redis.lpush("loo3", "1");
      const stream = redis.scanStream({
        type: "list",
      });
      stream.on("data", function (data) {
        keys = keys.concat(data);
      });
      stream.on("end", function () {
        expect(keys.sort()).to.eql(["loo1", "loo2", "loo3"]);
        redis.disconnect();
        done();
      });
    });

    it("should recognize `COUNT`", function (done) {
      let keys = [];
      const redis = new Redis();
      sinon.spy(Redis.prototype, "scan");
      redis.mset(
        "foo1",
        1,
        "foo2",
        1,
        "foo3",
        1,
        "foo4",
        1,
        "foo10",
        1,
        function () {
          const stream = redis.scanStream({
            count: 2,
          });
          stream.on("data", function (data) {
            keys = keys.concat(data);
          });
          stream.on("end", function () {
            expect(keys.sort()).to.eql([
              "foo1",
              "foo10",
              "foo2",
              "foo3",
              "foo4",
            ]);
            const [args] = Redis.prototype.scan.getCall(0).args;
            let count;
            for (let i = 0; i < args.length; ++i) {
              if (
                typeof args[i] === "string" &&
                args[i].toUpperCase() === "COUNT"
              ) {
                count = args[i + 1];
                break;
              }
            }
            expect(count).to.eql("2");
            redis.disconnect();
            done();
          });
        }
      );
    });

    it("should emit an error when connection is down", function (done) {
      let keys = [];
      const redis = new Redis();
      redis.mset(
        "foo1",
        1,
        "foo2",
        1,
        "foo3",
        1,
        "foo4",
        1,
        "foo10",
        1,
        function () {
          redis.disconnect();
          const stream = redis.scanStream({ count: 1 });
          stream.on("data", function (data) {
            keys = keys.concat(data);
          });
          stream.on("error", function (err) {
            expect(err.message).to.eql(
              "scanStream failed: Connection is closed."
            );
            done();
          });
        }
      );
    });
  });

  describe("scanBufferStream", function () {
    it("should return buffer", function (done) {
      let keys = [];
      const redis = new Redis();
      redis.mset(
        "foo1",
        1,
        "foo2",
        1,
        "foo3",
        1,
        "foo4",
        1,
        "foo10",
        1,
        function () {
          const stream = redis.scanBufferStream();
          stream.on("data", function (data) {
            keys = keys.concat(data);
          });
          stream.on("end", function () {
            expect(keys.sort()).to.eql([
              Buffer.from("foo1"),
              Buffer.from("foo10"),
              Buffer.from("foo2"),
              Buffer.from("foo3"),
              Buffer.from("foo4"),
            ]);
            redis.disconnect();
            done();
          });
        }
      );
    });
  });

  describe("sscanStream", function () {
    it("should iterate all values in the set", function (done) {
      let keys = [];
      const redis = new Redis();
      redis.sadd("set", "foo1", "foo2", "foo3", "foo4", "foo10", function () {
        const stream = redis.sscanStream("set", { match: "foo??" });
        stream.on("data", function (data) {
          keys = keys.concat(data);
        });
        stream.on("end", function () {
          expect(keys).to.eql(["foo10"]);
          redis.disconnect();
          done();
        });
      });
    });
  });

  describe("Cluster", function () {
    it("should work in cluster mode", function (done) {
      const slotTable = [
        [0, 5460, ["127.0.0.1", 30001]],
        [5461, 10922, ["127.0.0.1", 30002]],
        [10923, 16383, ["127.0.0.1", 30003]],
      ];
      const serverKeys = ["foo1", "foo2", "foo3", "foo4", "foo10"];
      const argvHandler = function (argv) {
        if (argv[0] === "cluster" && argv[1] === "slots") {
          return slotTable;
        }
        if (argv[0] === "sscan" && argv[1] === "set") {
          const cursor = Number(argv[2]);
          if (cursor >= serverKeys.length) {
            return ["0", []];
          }
          return [String(cursor + 1), [serverKeys[cursor]]];
        }
      };
      new MockServer(30001, argvHandler);
      new MockServer(30002, argvHandler);
      new MockServer(30003, argvHandler);

      const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }]);

      let keys = [];
      // @ts-ignore
      cluster.sadd("set", serverKeys, function () {
        // @ts-ignore
        const stream = cluster.sscanStream("set");
        stream.on("data", function (data) {
          keys = keys.concat(data);
        });
        stream.on("end", function () {
          expect(keys).to.eql(serverKeys);
          cluster.disconnect();
          done();
        });
      });
    });
  });
});
