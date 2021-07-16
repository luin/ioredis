import Redis from "../../lib/redis";
import MockServer from "../helpers/mock_server";
import { assert, expect } from "chai";
import * as sinon from "sinon";

const warn = sinon.spy(console, "warn");

describe("sanitizeErrors: true", () => {
  it(`should immutably omit command.args from emitted errors`, (done) => {
    const originalError: any = new Error("Wrong password");
    new MockServer(43215, ([name, ...args]) => {
      if (name === "auth") {
        originalError.command = { name, args };
        redis.emit("error", originalError);
        redis.disconnect();
      }
    });
    const redis = new Redis({
      password: "secret",
      port: 43215,
      sanitizeErrors: true,
    });
    redis.once("error", (error) => {
      expect(error.command.name).to.eql("auth");
      expect(error.command.args).to.be.undefined;
      expect(typeof error).to.eql(typeof originalError);
      expect(originalError.command.args).to.eql(["secret"]);
      done();
    });
  });
});

describe("sanitizeErrors: false", () => {
  it("should not sanitize", (done) => {
    const originalError: any = new Error("Wrong password");
    new MockServer(43215, ([name, ...args]) => {
      if (name === "auth") {
        originalError.command = { name, args };
        redis.emit("error", originalError);
        redis.disconnect();
      }
    });
    const redis = new Redis({
      password: "secret",
      port: 43215,
      sanitizeErrors: false,
    });
    redis.once("error", (error) => {
      expect(error).to.equal(originalError);
      expect(error.command.name).to.eql("auth");
      expect(error.command.args).to.eql(["secret"]);
      done();
    });
  });
});

describe("sanitizeErrors: not specified", () => {
  it("should not sanitize", (done) => {
    const originalError: any = new Error("Wrong password");
    new MockServer(43215, ([name, ...args]) => {
      if (name === "auth") {
        originalError.command = { name, args };
        redis.emit("error", originalError);
        redis.disconnect();
      }
    });
    const redis = new Redis({
      password: "secret",
      port: 43215,
    });
    redis.once("error", (error) => {
      expect(error).to.equal(originalError);
      expect(error.command.name).to.eql("auth");
      expect(error.command.args).to.eql(["secret"]);
      done();
    });
  });
  it("should warn the user", () => {
    assert(
      warn.calledOnceWith(
        "[WARN] In the future, command args will automatically be sanitized away from error objects. To keep the current behavior, set sanitizeErrors: false when configuring ioredis"
      )
    );
    warn.restore();
  });
});
