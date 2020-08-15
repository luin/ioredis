import MockServer from "../../helpers/mock_server";
import { expect } from "chai";
import { Cluster } from "../../../lib";
import * as sinon from "sinon";

describe("cluster:connect", function () {
  it("should flush the queue when all startup nodes are unreachable", function (done) {
    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      clusterRetryStrategy: null,
    });

    cluster.get("foo", function (err) {
      expect(err.message).to.match(/None of startup nodes is available/);
      cluster.disconnect();
      done();
    });
  });

  it("should invoke clusterRetryStrategy when all startup nodes are unreachable", function (done) {
    let t = 0;
    const cluster = new Cluster(
      [
        { host: "127.0.0.1", port: "30001" },
        { host: "127.0.0.1", port: "30002" },
      ],
      {
        clusterRetryStrategy: function (times) {
          expect(times).to.eql(++t);
          if (times === 3) {
            return;
          }
          return 0;
        },
      }
    );

    cluster.get("foo", function (err) {
      expect(t).to.eql(3);
      expect(err.message).to.match(/None of startup nodes is available/);
      cluster.disconnect();
      done();
    });
  });

  it("should invoke clusterRetryStrategy when none nodes are ready", function (done) {
    const argvHandler = function (argv) {
      if (argv[0] === "cluster") {
        return new Error("CLUSTERDOWN");
      }
    };
    new MockServer(30001, argvHandler);
    new MockServer(30002, argvHandler);

    let t = 0;
    var cluster = new Cluster(
      [
        { host: "127.0.0.1", port: "30001" },
        { host: "127.0.0.1", port: "30002" },
      ],
      {
        clusterRetryStrategy: function (times) {
          expect(times).to.eql(++t);
          if (times === 3) {
            cluster.disconnect();
            done();
            return;
          }
          return 0;
        },
      }
    );
  });

  it("should connect to cluster successfully", function (done) {
    const node = new MockServer(30001);

    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }]);

    node.once("connect", function () {
      cluster.disconnect();
      done();
    });
  });

  it("should wait for ready state before resolving", function (done) {
    const slotTable = [[0, 16383, ["127.0.0.1", 30001]]];
    const argvHandler = function (argv) {
      if (argv[0] === "info") {
        // return 'role:master'
      }
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }
      if (argv[0] === "cluster" && argv[1] === "info") {
        return "cluster_state:ok";
      }
    };
    new MockServer(30001, argvHandler);

    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      lazyConnect: true,
    });

    cluster.connect().then(function () {
      expect(cluster.status).to.eql("ready");
      cluster.disconnect();
      done();
    });
  });

  it("should support url schema", function (done) {
    const node = new MockServer(30001);

    const cluster = new Cluster(["redis://127.0.0.1:30001"]);

    node.once("connect", function () {
      cluster.disconnect();
      done();
    });
  });

  it("should support a single port", function (done) {
    const node = new MockServer(30001);

    const cluster = new Cluster([30001]);

    node.once("connect", function () {
      cluster.disconnect();
      done();
    });
  });

  it("should return a promise to be resolved when connected", function (done) {
    const slotTable = [
      [0, 5460, ["127.0.0.1", 30001]],
      [5461, 10922, ["127.0.0.1", 30002]],
      [10923, 16383, ["127.0.0.1", 30003]],
    ];
    const argvHandler = function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }
    };
    new MockServer(30001, argvHandler);
    new MockServer(30002, argvHandler);
    new MockServer(30003, argvHandler);

    sinon.stub(Cluster.prototype, "connect").callsFake(() => Promise.resolve());
    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      lazyConnect: false,
    });
    Cluster.prototype.connect.restore();

    cluster.connect().then(function () {
      cluster.disconnect();
      done();
    });
  });

  it("should return a promise to be rejected when closed", function (done) {
    sinon.stub(Cluster.prototype, "connect").callsFake(() => Promise.resolve());
    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      lazyConnect: false,
    });
    Cluster.prototype.connect.restore();

    cluster.connect().catch(function () {
      cluster.disconnect();
      done();
    });
  });

  it("should stop reconnecting when disconnected", function (done) {
    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      clusterRetryStrategy: function () {
        return 0;
      },
    });

    cluster.on("close", function () {
      cluster.disconnect();
      const stub = sinon
        .stub(Cluster.prototype, "connect")
        .throws(new Error("`connect` should not be called"));
      setTimeout(function () {
        stub.restore();
        done();
      }, 1);
    });
  });

  it("should discover other nodes automatically", function (done) {
    const slotTable = [
      [0, 5460, ["127.0.0.1", 30001]],
      [5461, 10922, ["127.0.0.1", 30002]],
      [10923, 16383, ["127.0.0.1", 30003]],
    ];
    const argvHandler = function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }
    };
    const node1 = new MockServer(30001, argvHandler);
    const node2 = new MockServer(30002, argvHandler);
    const node3 = new MockServer(30003, argvHandler);

    let pending = 3;
    node1.once("connect", check);
    node2.once("connect", check);
    node3.once("connect", check);

    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      redisOptions: { lazyConnect: false },
    });

    function check() {
      if (!--pending) {
        cluster.disconnect();
        done();
      }
    }
  });

  it("should send command to the correct node", function (done) {
    new MockServer(30001, function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return [
          [0, 1, ["127.0.0.1", 30001]],
          [2, 16383, ["127.0.0.1", 30002]],
        ];
      }
    });
    new MockServer(30002, function (argv) {
      if (argv[0] === "get" && argv[1] === "foo") {
        process.nextTick(function () {
          cluster.disconnect();
          done();
        });
      }
    });

    var cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      lazyConnect: false,
    });
    cluster.get("foo");
  });

  it("should emit errors when cluster cannot be connected", function (done) {
    const errorMessage = "ERR This instance has cluster support disabled";
    const argvHandler = function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return new Error(errorMessage);
      }
    };
    new MockServer(30001, argvHandler);
    new MockServer(30002, argvHandler);

    let pending = 2;
    let retry: number | false = 0;
    var cluster = new Cluster(
      [
        { host: "127.0.0.1", port: "30001" },
        { host: "127.0.0.1", port: "30002" },
      ],
      {
        clusterRetryStrategy: function () {
          cluster.once("error", function (err) {
            retry = false;
            expect(err.message).to.eql("Failed to refresh slots cache.");
            expect(err.lastNodeError.message).to.eql(errorMessage);
            checkDone();
          });
          return retry;
        },
      }
    );

    cluster.once("node error", function (err, key) {
      expect(err.message).to.eql(errorMessage);
      expect(["127.0.0.1:30001", "127.0.0.1:30002"]).to.include(key);
      checkDone();
    });

    function checkDone() {
      if (!--pending) {
        cluster.disconnect();
        done();
      }
    }
  });

  it("Show reconnect closed nodes without draining cluster", function (done) {
    const slotTable = [
      [0, 12181, ["127.0.0.1", 30001]],
      [12182, 12183, ["127.0.0.1", 30002]],
      [12184, 16383, ["127.0.0.1", 30001]],
    ];

    new MockServer(30001, function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }
    });

    let server = new MockServer(30002, function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }
    });

    const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      reconnectClosedNodes: true,
    });

    cluster.on("close", () => {
      // If cluster is ever closed, fail the test
      done(new Error("Cluster closed"));
    });

    // First time connection established to both nodes
    cluster.get("foo", function () {
      // Shutdown destination node
      server.disconnect(function () {
        // Query sent to closed node, trigger -node event, and node reset to wait status
        cluster.get("foo", function () {
          // Restart destination node
          server = new MockServer(30002, function (argv) {
            if (argv[0] === "cluster" && argv[1] === "slots") {
              return slotTable;
            }
          });
          // Seamlessly reconnect to the node without retry whole cluster.
          cluster.get("foo", function () {
            done();
          });
        });
      });
    });
  });

  it("should using the specified password", function (done) {
    let cluster;
    const slotTable = [
      [0, 5460, ["127.0.0.1", 30001]],
      [5461, 10922, ["127.0.0.1", 30002]],
      [10923, 16383, ["127.0.0.1", 30003]],
    ];
    const argvHandler = function (port, argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        return slotTable;
      }
      if (argv[0] === "auth") {
        const password = argv[1];
        if (port === 30001) {
          expect(password).to.eql("other password");
        } else if (port === 30002) {
          throw new Error("30002 got password");
        } else if (port === 30003) {
          expect(password).to.eql("default password");
          cluster.disconnect();
          done();
        }
      }
    };
    new MockServer(30001, argvHandler.bind(null, 30001));
    new MockServer(30002, argvHandler.bind(null, 30002));
    new MockServer(30003, argvHandler.bind(null, 30003));

    cluster = new Cluster(
      [
        { host: "127.0.0.1", port: "30001", password: "other password" },
        { host: "127.0.0.1", port: "30002", password: null },
      ],
      { redisOptions: { lazyConnect: false, password: "default password" } }
    );
  });

  it("should discover other nodes automatically every slotsRefreshInterval", function (done) {
    let times = 0;
    let cluster;
    const argvHandler = function (argv) {
      if (argv[0] === "cluster" && argv[1] === "slots") {
        times++;
        if (times === 1) {
          return [
            [0, 5460, ["127.0.0.1", 30001]],
            [5461, 10922, ["127.0.0.1", 30001]],
            [10923, 16383, ["127.0.0.1", 30001]],
          ];
        }

        return [
          [0, 5460, ["127.0.0.1", 30001]],
          [5461, 10922, ["127.0.0.1", 30001]],
          [10923, 16383, ["127.0.0.1", 30002]],
        ];
      }
    };
    const node1 = new MockServer(30001, argvHandler);
    const node2 = new MockServer(30002, argvHandler);

    node1.once("connect", function () {
      node2.once("connect", function () {
        cluster.disconnect();
        done();
      });
    });

    cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
      slotsRefreshInterval: 100,
      redisOptions: { lazyConnect: false },
    });
  });

  it("throws when startupNodes is empty", (done) => {
    const message = "`startupNodes` should contain at least one node.";
    let pending = 2;
    const cluster = new Cluster(null, {
      lazyConnect: true,
      clusterRetryStrategy(_, reason) {
        expect(reason.message).to.eql(message);
        if (!--pending) {
          done();
        }
        return false;
      },
    });
    cluster.connect().catch((err) => {
      expect(err.message).to.eql(message);
      cluster.disconnect();
      if (!--pending) {
        done();
      }
    });
  });

  describe("multiple reconnect", function () {
    it("should reconnect after multiple consecutive disconnect(true) are called", function (done) {
      new MockServer(30001);
      const cluster = new Cluster([{ host: "127.0.0.1", port: "30001" }], {
        enableReadyCheck: false,
      });
      cluster.once("reconnecting", function () {
        cluster.disconnect(true);
      });
      cluster.once("ready", function () {
        cluster.disconnect(true);
        const rejectTimeout = setTimeout(function () {
          cluster.disconnect();
          done(new Error("second disconnect(true) didn't reconnect redis"));
        }, 1000);
        process.nextTick(function () {
          cluster.once("ready", function () {
            clearTimeout(rejectTimeout);
            cluster.disconnect();
            done();
          });
        });
      });
    });
  });
});
