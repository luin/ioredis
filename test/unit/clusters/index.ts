import { nodeKeyToRedisOptions, NodeRole } from "../../../lib/cluster/util";
import { Cluster } from "../../../lib";
import * as sinon from "sinon";
import { expect } from "chai";

describe("cluster", function () {
  let functionSpy: sinon.SinonStub;

  beforeEach(function () {
    functionSpy = sinon.stub(Cluster.prototype, "connect");
    functionSpy.callsFake(() => Promise.resolve());
  });

  afterEach(function () {
    //Cluster.prototype.connect.restore();
    functionSpy.restore();
  });

  it("should support frozen options", function () {
    const options = Object.freeze({ maxRedirections: 1000 });
    const cluster = new Cluster([{ port: 7777 }], options);
    // @ts-ignore
    expect(cluster.options).to.have.property("maxRedirections", 1000);
    // @ts-ignore
    expect(cluster.options).to.have.property("showFriendlyErrorStack", false);
    // @ts-ignore
    expect(cluster.options).to.have.property("scaleReads", "master");
  });

  it("should allow overriding Commander options", function () {
    const cluster = new Cluster([{ port: 7777 }], {
      // @ts-ignore
      showFriendlyErrorStack: true,
    });
    // @ts-ignore
    expect(cluster.options).to.have.property("showFriendlyErrorStack", true);
  });

  it("throws when scaleReads is invalid", function () {
    expect(function () {
      new Cluster([{}], { scaleReads: "invalid" as NodeRole });
    }).to.throw(/Invalid option scaleReads/);
  });

  describe("#nodes()", function () {
    it("throws when role is invalid", function () {
      const cluster = new Cluster([{}]);
      expect(function () {
        cluster.nodes("invalid" as NodeRole);
      }).to.throw(/Invalid role/);
    });
  });
});

describe("nodeKeyToRedisOptions()", () => {
  it("returns correct result", () => {
    expect(nodeKeyToRedisOptions("127.0.0.1:6379")).to.eql({
      port: 6379,
      host: "127.0.0.1",
    });
    expect(nodeKeyToRedisOptions("192.168.1.1:30001")).to.eql({
      port: 30001,
      host: "192.168.1.1",
    });
    expect(nodeKeyToRedisOptions("::0:6379")).to.eql({
      port: 6379,
      host: "::0",
    });
    expect(nodeKeyToRedisOptions("0:0:6379")).to.eql({
      port: 6379,
      host: "0:0",
    });
  });
});
