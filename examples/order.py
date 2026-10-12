"""End-to-end example: define an order-fulfilment workflow, run a worker, submit orders.

    # start a server first, e.g. the Docker image or `./gradlew :dist:run`
    python examples/order.py
"""
import os

from wiggle import FlowSpec, RetryPolicy, WiggleClient, Worker


def topology() -> FlowSpec:
    # The topology as a chain of named steps; nothing runs here. Handlers bind by name below.
    return FlowSpec.define("py-order", lambda f: f
                           .then_apply("validate")
                           .then_filter("in-stock")
                           .then_apply("charge", queue="payments", retry=RetryPolicy.exponential(5, 0.1))
                           .then_apply("ship")
                           .then_accept("notify"))


def bind(worker: Worker) -> Worker:
    # A worker implements steps by name; a step returns the whole context, the engine merges the diff.
    return (
        worker
        .handle("py-order", "validate", lambda o: {**o, "status": "VALIDATED"})
        .handle_gate("py-order", "in-stock", lambda o: o["quantity"] > 0)
        .handle("py-order", "charge", lambda o: {**o, "paymentRef": f"auth-{o['orderId']}"})
        .handle("py-order", "ship", lambda o: {**o, "trackingLabel": f"DHL-{o['orderId']}"})
        .handle_effect("py-order", "notify",
                       lambda o: print(f"   [worker] {o['orderId']} -> {o['status']} "
                                       f"paid={o.get('paymentRef')} tracking={o.get('trackingLabel')}"))
    )


def main() -> None:
    url = os.environ.get("WIGGLE_URL", "localhost:8080")
    wf = topology()
    with WiggleClient(url) as client:
        client.register(wf)
        worker = bind(Worker(client, "py-worker-1")).start()
        try:
            ids = [client.start(wf, {"orderId": f"A-{1000 + i}", "quantity": 1 + (i % 3)})
                   for i in range(5)]
            for iid in ids:
                view = client.await_completion(iid, timeout_s=30)
                print(f"  {iid}  {view.status}"
                      + (f"  ({view.termination_reason})" if view.termination_reason else "")
                      + (f"  ERROR: {view.error}" if view.error else ""))
        finally:
            worker.close()


if __name__ == "__main__":
    main()
