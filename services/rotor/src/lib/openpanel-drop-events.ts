import { JitsuFunction, FuncReturn, AnyEvent } from "@jitsu/protocols/functions";
import { getLog } from "juava";
import NodeCache from "node-cache";
import { Redis } from "ioredis";

const log = getLog("openpanel-drop-events");

// Local in-memory cache: "projectId:eventName" → boolean (isDropped)
// 5-second TTL: drop/undrop takes effect within 5s across all Rotor nodes
const droppedCache = new NodeCache({ stdTTL: 5, checkperiod: 2, useClones: false });

// Module-level Redis singleton — lazily initialized on first call
let redis: Redis | undefined;

function getRedis(): Redis {
  if (!redis) {
    const { createRedis } = require("./redis");
    redis = createRedis();
  }
  return redis;
}

function resolveEventName(event: AnyEvent): string | undefined {
  // Mirror the OpenPanel Bulker consumeMap() + field_mapper.go logic exactly:
  // Only "track" and "screen" produce OpenPanel event rows (stream.go:100-101).
  // All other types (identify, page, alias, group) → pass through, not droppable.
  const eventType = (event as any).type;
  if (eventType === "screen") return "screen_view";
  if (eventType === "track" || !eventType) return (event as any).event || "unknown";
  return undefined;
}

const OpenpanelDropEventsFunction: JitsuFunction = async (event: AnyEvent, ctx: any): Promise<FuncReturn> => {
  const projectId = ctx.props?.projectId;
  if (!projectId) return event;

  const eventName = resolveEventName(event);
  if (!eventName) return event;

  const cacheKey = `${projectId}:${eventName}`;
  const cached = droppedCache.get<boolean>(cacheKey);
  if (cached !== undefined) {
    return cached ? "drop" : event;
  }

  try {
    const isDropped = await getRedis().sismember(`op:dropped:${projectId}`, eventName);
    droppedCache.set(cacheKey, isDropped === 1);

    if (isDropped === 1) {
      log.atDebug().log(`Dropping event '${eventName}' for project ${projectId}`);
      return "drop";
    }
  } catch (e: any) {
    log.atWarn().withCause(e).log(`Redis check failed for '${eventName}', passing through`);
  }

  return event;
};

export default OpenpanelDropEventsFunction;
