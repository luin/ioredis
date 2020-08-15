import * as net from "net";
import * as tls from "tls";
import { StandaloneConnector } from "../../../lib/connectors";
import * as sinon from "sinon";
import { expect } from "chai";

describe("StandaloneConnector", () => {
  describe("connect()", () => {
    it("first tries path", async () => {
      const spy = sinon.spy(net, "createConnection");
      const connector = new StandaloneConnector({ port: 6379, path: "/tmp" });
      try {
        const stream = await connector.connect(() => {});
        stream.on("error", () => {});
      } catch (err) {
        // ignore errors
      }
      expect(spy.calledOnce).to.eql(true);
      connector.disconnect();
    });

    it("ignore path when port is set and path is null", async () => {
      const spy = sinon.stub(net, "createConnection");
      const connector = new StandaloneConnector({ port: 6379, path: null });
      await connector.connect(() => {});
      expect(spy.calledOnce).to.eql(true);
      expect(spy.firstCall.args[0]).to.eql({ port: 6379 });
      connector.disconnect();
    });

    it("supports tls", async () => {
      const spy = sinon.stub(tls, "connect");
      const connector = new StandaloneConnector({
        port: 6379,
        tls: { ca: "on" },
      });
      await connector.connect(() => {});
      expect(spy.calledOnce).to.eql(true);
      expect(spy.firstCall.args[0]).to.eql({ port: 6379, ca: "on" });
      connector.disconnect();
    });

    it("supports tls sni", async () => {
      const spy = sinon.stub(tls, "connect");
      const connector = new StandaloneConnector({
        host: "localhost",
        port: 6379,
        tlsSni: true,
      });
      await connector.connect(() => {});
      expect(spy.calledOnce).to.eql(true);
      expect(spy.firstCall.args[0]).to.eql({
        host: "localhost",
        port: 6379,
        servername: "localhost",
      });
      connector.disconnect();
    });
  });
});
