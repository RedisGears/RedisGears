from redis import ResponseError
from common import gearsTest, shardsConnections, toDictionary
from common import Env
from websockets.sync.client import connect
import json

# This adds logging. Uncomment to see the websocket logging.
# import logging
# logger = logging.getLogger('websockets')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

# This script is used everywhere within the test.
SCRIPT = '''#!js name=gears_test_script api_version=1.0
var redis = redis;

var numberOfCalls = 0;

redis.registerFunction("foo", () => {
    return ++numberOfCalls;
});

redis.registerAsyncFunction(
  'asyncfoo', //Function name
  async function(async_client, args) {
      console.log("Hello from async");
      return "OK"
  }, //callback
  {
    description: 'description',
    flags: [redis.functionFlags.NO_WRITES, redis.functionFlags.ALLOW_OOM]
  } //optional arguments
);

redis.registerKeySpaceTrigger(
  'bar', // trigger name
  'keys:*', //key prefix
  function(client, data) {
    console.log("Got this key data updated1: " + data);
  }, //callback
  {
    description: 'description',
    onTriggerFired: function(client, data) {
        console.log("Got this key data updated2: " + data);
    }
  } //optional arguments
);

redis.registerStreamTrigger(
  'foobar', //trigger name
  'stream:*', //prefix
  function(client, data) {
    console.log("Got this stream data updated: " + data);
  },//callback
  {
    description: 'Description',
    window: 1,
    isStreamTrimmed: false
  } //optional arguments
);
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZmlsZS5qcyIsInNvdXJjZXMiOlsiLi4vLi4vanNfYXBpL2dlYXJzLWFwaS5qcyIsImluZGV4LmpzIl0sInNvdXJjZXNDb250ZW50IjpbInZhciByZWRpcyA9IHJlZGlzO1xuXG5leHBvcnR7IHJlZGlzIH1cbiIsIiMhanMgbmFtZT1nZWFyc19leGFtcGxlIGFwaV92ZXJzaW9uPTEuMFxuXG5pbXBvcnQgeyByZWRpcyB9IGZyb20gJ0ByZWRpcy9nZWFycy1hcGknO1xuXG52YXIgbnVtYmVyT2ZDYWxscyA9IDA7XG5cbnJlZGlzLnJlZ2lzdGVyRnVuY3Rpb24oXCJmb29cIiwgKCkgPT4ge1xuICAgIHJldHVybiArK251bWJlck9mQ2FsbHM7XG59KTtcblxucmVkaXMucmVnaXN0ZXJBc3luY0Z1bmN0aW9uKFxuICAnYXN5bmNmb28nLCAvL0Z1bmN0aW9uIG5hbWVcbiAgZnVuY3Rpb24oY2xpZW50LCBhcmdzKSB7XG4gICAgICBjb25zb2xlLmxvZyhcIkhlbGxvIGZyb20gYXN5bmNcIilcbiAgfSwgLy9jYWxsYmFja1xuICB7XG4gICAgZGVzY3JpcHRpb246ICdkZXNjcmlwdGlvbicsXG4gICAgZmxhZ3M6IFtyZWRpcy5mdW5jdGlvbkZsYWdzLk5PX1dSSVRFUywgcmVkaXMuZnVuY3Rpb25GbGFncy5BTExPV19PT01dXG4gIH0gLy9vcHRpb25hbCBhcmd1bWVudHNcbik7XG5cbnJlZGlzLnJlZ2lzdGVyS2V5U3BhY2VUcmlnZ2VyKFxuICAnYmFyJywgLy8gdHJpZ2dlciBuYW1lXG4gICdrZXlzOionLCAvL2tleSBwcmVmaXhcbiAgZnVuY3Rpb24oY2xpZW50LCBkYXRhKSB7XG4gICAgY29uc29sZS5sb2coXCJHb3QgdGhpcyBrZXkgZGF0YSB1cGRhdGVkMTogXCIgKyBkYXRhKTtcbiAgfSwgLy9jYWxsYmFja1xuICB7XG4gICAgZGVzY3JpcHRpb246ICdkZXNjcmlwdGlvbicsXG4gICAgb25UcmlnZ2VyRmlyZWQ6IGZ1bmN0aW9uKGNsaWVudCwgZGF0YSkge1xuICAgICAgICBjb25zb2xlLmxvZyhcIkdvdCB0aGlzIGtleSBkYXRhIHVwZGF0ZWQyOiBcIiArIGRhdGEpO1xuICAgIH1cbiAgfSAvL29wdGlvbmFsIGFyZ3VtZW50c1xuKVxuXG5yZWRpcy5yZWdpc3RlclN0cmVhbVRyaWdnZXIoXG4gICdmb29iYXInLCAvL3RyaWdnZXIgbmFtZVxuICAnc3RyZWFtOionLCAvL3ByZWZpeFxuICBmdW5jdGlvbihjbGllbnQsIGRhdGEpIHtcbiAgICBjb25zb2xlLmxvZyhcIkdvdCB0aGlzIHN0cmVhbSBkYXRhIHVwZGF0ZWQ6IFwiICsgZGF0YSk7XG4gIH0sLy9jYWxsYmFja1xuICB7XG4gICAgZGVzY3JpcHRpb246ICdEZXNjcmlwdGlvbicsXG4gICAgd2luZG93OiAxLFxuICAgIGlzU3RyZWFtVHJpbW1lZDogZmFsc2VcbiAgfSAvL29wdGlvbmFsIGFyZ3VtZW50c1xuKVxuIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7QUFBQSxDQUFJLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBSyxHQUFHLENBQUssQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBOztBQ0lqQixDQUFJLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQWEsQ0FBRyxDQUFBLENBQUEsQ0FBQyxDQUFDO0FBQ3RCO0FBQ0EsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFLLENBQUMsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBZ0IsQ0FBQyxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUssRUFBRSxDQUFNLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBO0FBQ3BDLENBQUksQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBTyxDQUFFLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBYSxDQUFDO0FBQzNCLENBQUMsQ0FBQyxDQUFDO0FBQ0g7QUFDQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUssQ0FBQyxDQUFxQixDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUMzQixDQUFBLENBQUUsQ0FBVSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDWixDQUFBLENBQUUsQ0FBUyxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQU0sQ0FBRSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUksQ0FBRSxDQUFBLENBQUE7QUFDekIsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQU0sQ0FBTyxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFDLENBQUcsQ0FBQSxDQUFBLENBQUMsa0JBQWtCLENBQUMsQ0FBQTtBQUNyQyxDQUFHLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUNILENBQUUsQ0FBQSxDQUFBO0FBQ0YsQ0FBSSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQVcsRUFBRSxDQUFhLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDOUIsQ0FBQSxDQUFBLENBQUEsQ0FBSSxDQUFLLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFFLENBQUMsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFLLENBQUMsQ0FBYSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFDLENBQVMsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBRSxDQUFLLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQyxDQUFhLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUMsU0FBUyxDQUFDO0FBQ3pFLENBQUcsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBO0FBQ0gsQ0FBQyxDQUFDO0FBQ0Y7QUFDQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUssQ0FBQyxDQUF1QixDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDN0IsQ0FBQSxDQUFFLENBQUssQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDUCxDQUFBLENBQUUsQ0FBUSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUNWLENBQUEsQ0FBRSxDQUFTLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBTSxDQUFFLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBSSxDQUFFLENBQUEsQ0FBQTtBQUN6QixDQUFJLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQU8sQ0FBQyxDQUFHLENBQUEsQ0FBQSxDQUFDLDhCQUE4QixDQUFHLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFJLENBQUMsQ0FBQztBQUN2RCxDQUFHLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUNILENBQUUsQ0FBQSxDQUFBO0FBQ0YsQ0FBSSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQVcsRUFBRSxDQUFhLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDOUIsQ0FBQSxDQUFBLENBQUEsQ0FBSSxjQUFjLENBQUUsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBUyxDQUFNLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUUsSUFBSSxDQUFFLENBQUEsQ0FBQTtBQUMzQyxDQUFRLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBTyxDQUFDLENBQUcsQ0FBQSxDQUFBLENBQUMsOEJBQThCLENBQUcsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUksQ0FBQyxDQUFDO0FBQzNELENBQUssQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUNMLENBQUcsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBO0FBQ0gsQ0FBQyxDQUFBO0FBQ0Q7QUFDQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUssQ0FBQyxDQUFxQixDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUMzQixDQUFBLENBQUUsQ0FBUSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDVixDQUFBLENBQUUsQ0FBVSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBO0FBQ1osQ0FBQSxDQUFFLENBQVMsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFNLENBQUUsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFJLENBQUUsQ0FBQSxDQUFBO0FBQ3pCLENBQUksQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBTyxDQUFDLENBQUcsQ0FBQSxDQUFBLENBQUMsZ0NBQWdDLENBQUcsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUksQ0FBQyxDQUFDO0FBQ3pELENBQUcsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQTtBQUNILENBQUUsQ0FBQSxDQUFBO0FBQ0YsQ0FBSSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQVcsRUFBRSxDQUFhLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDOUIsQ0FBSSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBTSxFQUFFLENBQUMsQ0FBQTtBQUNiLENBQUksQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQWUsRUFBRSxDQUFLLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDMUIsQ0FBRyxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUEsQ0FBQSxDQUFBLENBQUE7QUFDSCxDQUFBIn0=
'''

"""
A short list of debugger client messages following the V8 Inspector API.

For the definition:

<https://chromedevtools.github.io/devtools-protocol/tot/>
"""
class DebuggerClientMessage:
    RUNTIME_ENABLE = {
        "method": "Runtime.enable",
        "params": {}
    }

    DEBUGGER_ENABLE = {
        "method": "Debugger.enable",
        "params": {}
    }

    DEBUGGER_RESUME = {
        "method": "Debugger.resume",
        "params": {}
    }

    DEBUGGER_STEP_INTO = {
        "method": "Debugger.stepInto",
        "params": {}
    }

    DEBUGGER_STEP_OUT = {
        "method": "Debugger.stepOut",
        "params": {}
    }

    DEBUGGER_STEP_OVER = {
        "method": "Debugger.stepOver",
        "params": {}
    }

    RUNTIME_RUN_IF_WAITING_FOR_DEBUGGER = {
        "method": "Runtime.runIfWaitingForDebugger",
        "params": {}
    }

    """
    Generates a V8 RPC message for setting a breakpoint.
    """
    @staticmethod
    def set_breakpoint(line_number: int, column_number: int, script_hash: str) -> dict:
        return {
            "method": "Debugger.setBreakpointByUrl",
            "params": {
                "lineNumber": line_number,
                "scriptHash": script_hash,
                "columnNumber": column_number,
                "condition": ""
            }
        }

    """
    Generates a V8 RPC message for removing a breakpoint by its id.
    """
    @staticmethod
    def remove_breakpoint(breakpoint_id: int) -> dict:
        return {
            "method": "Debugger.removeBreakpoint",
            "params": {
                "breakpointId": breakpoint_id,
            }
        }

"""
A remote debugger breakpoint.
"""
class Breakpoint:
    def __init__(self, client, data: dict) -> None:
        self._client = client
        self.data = data

    """
    Removes this breakpoint.
    """
    def remove(self):
        self._client.remove_breakpoint(self.get_id())

    """
    Returns the breakpoint id.
    """
    def get_id(self) -> str:
        return self.data["breakpointId"]

"""
The remote debugger client is a web socket client that uses the
V8 Inspector JSON RPC API.
"""
class DebuggerClient:
    """
    The default websocket address for RedisGears V8 plugin debugger
    server.
    """
    DEFAULT_WEBSOCKET_ADDRESS = "ws://127.0.0.1:9005"

    """
    Instantiates a new debugger client.
    If the address is omitted, the default one is used.
    """
    def __init__(self, env: Env, address=None) -> None:
        self._last_message_id = 1
        self._env = env
        self.breakpoints = []
        self.pause = None
        self.scripts_parsed = []
        self._websocket = connect(address if address is not None else DebuggerClient.DEFAULT_WEBSOCKET_ADDRESS)
        self.initiate_startup()

    """
    Initiates the debugger session start. We need to invoke a few
    methods in order for the remote server to start. The most important
    one to start is the "Runtime.enable" method.

    After all the startup methods are received, the remote debugger
    initialises a session, prepares stuff and so on. After all of it is
    done, we need to tell the remote debugger server that we are ready
    and not going to invoke any more initialisation methods for
    setting up the environment, by calling the
    "Runtime.runIfWaitingForDebugger" method.
    """
    def initiate_startup(self):
        self.runtime_enable()
        self.debugger_enable()
        self.run_if_waiting_for_debugger()

    """
    Disconnects from the remote server.
    """
    def disconnect(self):
        self._websocket.close_socket()

    """
    Invokes the "Runtime.enable" method.
    """
    def runtime_enable(self):
        responses = self.send_message(DebuggerClientMessage.RUNTIME_ENABLE)
        self._env.assertEqual('Runtime.executionContextCreated', responses[0]["method"])
        self._env.assertEqual({}, responses[1]["result"])

    """
    Invokes the "Debugger.enable" method.
    """
    def debugger_enable(self):
        responses = self.send_message(DebuggerClientMessage.DEBUGGER_ENABLE)
        # Should contain at least one script parsed.
        self._env.assertEqual('Debugger.scriptParsed', responses[0]["method"])
        for r in responses:
            if "method" in r and r["method"] == "Debugger.scriptParsed":
                self.scripts_parsed.append(r["params"])
        self._env.assertContains("result", responses[-1])

    """
    Invokes the "Runtime.runIfWaitingForDebugger" method.
    """
    def run_if_waiting_for_debugger(self):
        responses = self.send_message(DebuggerClientMessage.RUNTIME_RUN_IF_WAITING_FOR_DEBUGGER )
        self._env.assertEqual(len(responses), 1)
        self._env.assertEqual({}, responses[0]["result"])
        # Right after that we should receive a "Debugger.paused" event.
        # The reason is that the v8 plugin pauses immediately on the
        # very first instruction of the user script, so that the user
        # can't miss anything until the debugger is attached and the
        # user can see the execution happening.
        pause = self.receive_event()
        self._env.assertEqual("Debugger.paused", pause["method"])
        self.pause = pause

    """
    Receives a single event from the server.
    """
    def receive_event(self, timeout: float = None) -> dict:
        return json.loads(self._websocket.recv(timeout))

    """
    Converts a python dictionary to a json dictionary, adds the
    mandatory "id" field, sends the message to the remote debugger
    server and receives the response(s) to it.

    This method accumulates all the responses from the server until a
    message with the id that of the client message is received.
    """
    def send_message(self, message: dict) -> [dict]:
        message["id"] = self._last_message_id
        self._websocket.send(json.dumps(message))
        responses = []
        while True:
            response = self.receive_event()
            if "id" in response:
                if response["id"] <= self._last_message_id:
                    responses.append(response)
                    if response["id"] == self._last_message_id:
                        self._last_message_id += 1
                        break
            else:
                responses.append(response)
        return responses

    """
    Sets a breakpoint at the specified location in the script. If the
    `script_hash` argument is omitted, the first parsed script is
    going to be used.
    """
    def set_breakpoint(self, line_number: int, column_number: int, script_hash: str = None) -> Breakpoint:
        script_hash = script_hash if script_hash is not None else self.scripts_parsed[0]["hash"]
        message = DebuggerClientMessage.set_breakpoint(line_number, column_number, script_hash)
        response = self.send_message(message)
        self._env.assertContains("result", response[0])
        self._env.assertContains("breakpointId", response[0]["result"])
        # The resolved locations number must be greater than zero. If
        # it is zero it indicates that the debugger couldn't resolve the
        # breakpoint to any point in the script.
        self._env.assertGreater(len(response[0]["result"]["locations"]), 0)
        breakpoint = Breakpoint(self, response[0]["result"])
        self.breakpoints.append(breakpoint)
        return breakpoint

    """
    Removes the breakpoint with the id passed.
    """
    def remove_breakpoint(self, breakpoint_id: str):
        response = self.send_message(DebuggerClientMessage.remove_breakpoint(breakpoint_id))
        self._env.assertContains("result", response[0])
        self._env.assertEqual({}, response[0]["result"])

    """
    Resumes the execution (if it was paused) and returns all the events
    in response to the resume request.
    """
    def resume(self) -> [dict]:
        self.pause = None
        return self.send_message(DebuggerClientMessage.DEBUGGER_RESUME)

    """
    Resumes the execution (if it was paused), waits until the next
    pause happens and returns all the messages in between.
    """
    def resume_and_wait_until_pauses(self) -> [dict]:
        messages = self.resume()
        while True:
            message = self.receive_event()
            messages.append(message)
            if "method" in message and message["method"] == "Debugger.paused":
                self.pause = message
                break
        return messages

    """
    Debugger: step into the stack frame.
    """
    def step_into(self) -> [dict]:
        return self.send_message(DebuggerClientMessage.DEBUGGER_STEP_INTO)

    """
    Debugger: step out of the current stack frame.
    """
    def step_out(self) -> [dict]:
        return self.send_message(DebuggerClientMessage.DEBUGGER_STEP_OUT)

    """
    Debugger: step over the next instruction.
    """
    def step_over(self) -> [dict]:
        return self.send_message(DebuggerClientMessage.DEBUGGER_STEP_OVER)


def deploy_script(env: Env):
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', 'debug', SCRIPT).contains('The V8 remote debugging server is waiting for a connection')

@gearsTest(debugServerAddress="127.0.0.1:9005")
def testDeployConnectStartDisconnect(env: Env):
    deploy_script(env)
    client = DebuggerClient(env)
    client.disconnect()

    # Check that Redis still operates fine.
    env.expect('PING').equal(True)
    # After debugging the library should be deleted automatically from
    # the database, as the only purpose it was uploaded for was to have
    # a debugging session.
    env.assertEqual(len(toDictionary(env.cmd('TFUNCTION', 'list', 'library', 'lib', 'v'))), 0)


@gearsTest(debugServerAddress="127.0.0.1:9005")
def testSetBreakpointAndHitIt(env: Env):
    deploy_script(env)
    client = DebuggerClient(env)
    breakpoint = client.set_breakpoint(2, 1)
    env.assertEqual(len(client.breakpoints), 1)
    # Now proceed from the very first instruction to the very first
    # breakpoint we've just set.
    client.resume_and_wait_until_pauses()
    env.assertIsNotNone(client.pause)
    env.assertContains("callFrames", client.pause["params"])
    # Check that we hit the breakpoint we set above.
    env.assertEqual(breakpoint.get_id(), client.pause["params"]["hitBreakpoints"][0])

    client.disconnect()

    # Check that Redis still operates fine.
    env.expect('PING').equal(True)

@gearsTest(debugServerAddress="127.0.0.1:9005")
def testSetBreakpointAndRemoveIt(env: Env):
    deploy_script(env)
    client = DebuggerClient(env)
    breakpoint = client.set_breakpoint(2, 1)
    env.assertEqual(len(client.breakpoints), 1)
    breakpoint.remove()
    # Now proceed from the very first instruction till the end.
    env.assertEqual({}, client.resume()[0]["result"])
    # The next message should be a "resumed" event from the server.
    resumed_event = client.receive_event()
    env.assertEqual("Debugger.resumed", resumed_event["method"])
    # Now wait for the possible pause event to happen. It should not
    # happen now, for we have no breakpoints set, and the script
    # execution should just continue with no problem. We wait for
    # five seconds to let *any* sort of execution to continue on any VM,
    # so to avoid the flakiness of the test result.
    try:
        env.assertIsNone(client.receive_event(5.0))
    except TimeoutError:
        # Receiving a timeout error means there was no debugger pause,
        # exactly what we expect in this case.
        pass
    except BaseException as e:
        # We should not receive, however, any other sorts of error.
        raise(e)

    # Check that Redis still operates fine.
    env.expect('PING').equal(True)

    client.disconnect()

    # Check that Redis still operates fine.
    env.expect('PING').equal(True)

"""
The debugging is considered disabled, when the debug server
address argument is not correctly specified.
"""
@gearsTest()
def testNotPossibleToDebugWhenDebuggingIsDisabled(env: Env):
    env.expect('TFUNCTION', 'LOAD', 'REPLACE', 'debug', SCRIPT).contains('The debug server address was not specified in the configuration.')

def assert_load_for_debug_fails(env: Env, connection):
    try:
        connection.execute_command('TFUNCTION', 'DELETE', 'foo')
    except ResponseError as e:
        env.assertEqual(str(e), 'library does not exists')
    except BaseException:
        env.assertTrue(False, message="Didn't fail when it should have.")

    try:
        connection.execute_command('TFUNCTION', 'LOAD', 'debug', SCRIPT)
    except ResponseError as e:
        env.assertEqual("Debugging when in cluster mode isn't supported.", str(e))
    except BaseException:
        env.assertTrue(False, message="Didn't fail when it should have.")

@gearsTest(debugServerAddress="127.0.0.1:9005", cluster=True)
def testNotPossibleWhenInClusterMode(env: Env, master_connection):
    for shard_connection in shardsConnections(env):
        assert_load_for_debug_fails(env, shard_connection)

@gearsTest(debugServerAddress="127.0.0.1:9005", withReplicas=True)
def testNotPossibleOnReplicas(env: Env):
    slave_connection = env.getSlaveConnection()
    assert_load_for_debug_fails(env, slave_connection)

"""
The async functions are ran in the background, while the processing
happens in cron. Let's see Redis can still work fine.
"""
@gearsTest(debugServerAddress="127.0.0.1:9005")
def testDebuggingAsyncFunctionInBackground(env: Env):
    deploy_script(env)
    client = DebuggerClient(env)
    breakpoint = client.set_breakpoint(14, 1)
    env.assertEqual(len(client.breakpoints), 1)
    # Calling an asynchronous function shouldn't block the client.
    future = env.noBlockingTfcallAsync('gears_test_script', 'asyncfoo')
    # Now proceed from the very first instruction to the very first
    # breakpoint we've just set.
    client.resume_and_wait_until_pauses()
    env.assertIsNotNone(client.pause)
    env.assertContains("callFrames", client.pause["params"])
    # Check that we hit the breakpoint we set above.
    env.assertEqual(breakpoint.get_id(), client.pause["params"]["hitBreakpoints"][0])
    breakpoint.remove()
    client.resume()
    future.equal("OK")
    client.disconnect()

@gearsTest(debugServerAddress="127.0.0.1:9005")
def testKeySpaceTriggerPaused(env: Env):
    import threading
    import time

    deploy_script(env)
    client = DebuggerClient(env)
    breakpoint = client.set_breakpoint(26, 1)
    env.assertEqual(len(client.breakpoints), 1)

    def debugger_client():
        client.resume_and_wait_until_pauses()

        env.assertIsNotNone(client.pause)
        env.assertContains("callFrames", client.pause["params"])
        # Check that we hit the breakpoint we set above.
        env.assertEqual(breakpoint.get_id(), client.pause["params"]["hitBreakpoints"][0])

        breakpoint.remove()
        client.disconnect()

    thread = threading.Thread(target=debugger_client)
    thread.start()

    # Let's give the debugger-client thread a bit of a time.
    time.sleep(2)
    env.expect('SET', 'keys:*', 'value').equal(True)
    thread.join()

@gearsTest(debugServerAddress="127.0.0.1:9005")
def testStreamTriggerPaused(env: Env):
    import threading
    import time

    deploy_script(env)
    client = DebuggerClient(env)
    breakpoint = client.set_breakpoint(43, 1)
    env.assertEqual(len(client.breakpoints), 1)

    def debugger_client():
        client.resume_and_wait_until_pauses()

        env.assertIsNotNone(client.pause)
        env.assertContains("callFrames", client.pause["params"])
        # Check that we hit the breakpoint we set above.
        env.assertEqual(breakpoint.get_id(), client.pause["params"]["hitBreakpoints"][0])

        breakpoint.remove()
        client.disconnect()

    thread = threading.Thread(target=debugger_client)
    thread.start()

    # Let's give the debugger-client thread a bit of a time.
    time.sleep(2)
    env.expect('XADD', 'stream:1', '*', 'foo', 'bar')
    thread.join()
