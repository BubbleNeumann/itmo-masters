import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv


def debug_print(*args, **kwargs):
    kwargs['file'] = sys.stderr
    kwargs['flush'] = True
    print(*args, **kwargs)


env_path = Path('/app/.env')
if env_path.exists():
    load_dotenv(env_path)
    debug_print("Loaded .env from /app/.env")
else:
    debug_print("No .env file found")

from . import planner, validator, executor, utils


async def run_agent():
    debug_print("run_agent() started")
    history = []

    while True:
        try:
            user_input = input(">> ").strip()

            if not user_input:
                continue

            if user_input.lower() in ("exit", "quit"):
                print("Exiting...", flush=True)
                break

            history.append({"role": "user", "content": user_input})
            request_id = utils.new_request_id()

            try:
                debug_print("Calling planner.make_plan()")
                plan = planner.make_plan(history)
                debug_print(f"Plan received: {plan}")
            except Exception as e:
                debug_print(f"Planning error: {e}")
                import traceback
                traceback.print_exc(file=sys.stderr)
                continue

            clarify = plan.get("clarify", [])
            if clarify:
                for q in clarify:
                    if isinstance(q, dict):
                        print(f"clarify: {q.get('question', '')}")
                    else:
                        print(f"clarify: {q}")
                continue

            errors = validator.check_prerequisites(plan)
            if errors:
                for error in errors:
                    print(f"  - {error}")
                continue

            actions = plan.get("actions", [])
            if not actions:
                print("No actions to execute")
                continue

            print(f"exec {len(actions)} actions...")
            results = []

            for action in actions:
                try:
                    debug_print(f"Executing action: {action}")

                    if action["tool"] == "kafka_create_topic":
                        topic = action["args"].get("topic")
                        from .schema_validator import EVENT_SCHEMA, SchemaValidator
                        valid, msg = SchemaValidator.validate_schema(topic, EVENT_SCHEMA)
                        if not valid:
                            raise Exception(f"Schema validation failed: {msg}")

                    res = await executor.execute_action(action, request_id)
                    results.append({"tool": action["tool"], "status": "OK", "output": res})
                    debug_print(f"Action succeeded: {action['tool']}")
                except Exception as e:
                    results.append({"tool": action["tool"], "status": "ERROR", "error": str(e)})
                    debug_print(f"Action failed: {action['tool']} - {e}")
                    break

            print(f"request {request_id} completed")
            for r in results:
                if r['status'] == "OK":
                    print(f"  {r['tool']}: {r['status']}")
                    if 'output' in r and r['output']:
                        output = r['output']
                        if isinstance(output, dict):
                            if 'topics' in output:
                                print(f"topics: {', '.join(output['topics'])}")
                            else:
                                print(f"output: {output}")
                        # else:
                        #     print(f"output: {output}")
                else:
                    print(f"{r['tool']}: {r['status']} - {r.get('error', '')}")

            summary = f"executed {len(results)} actions."
            history.append({"role": "assistant", "content": summary})

        except KeyboardInterrupt:
            print("\nexiting...", flush=True)
            break
        except EOFError:
            print("\nconnection closed", flush=True)
            break
        except Exception as e:
            debug_print(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc(file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(run_agent())