# Examples

Four queries beyond the refund walkthrough in the [README](README.md). The screenshots are the SQL. The tables under them are simulated answers for the row described in each section. They are not a live model call.

## Is this a customer-facing outage?

`page` is `Checkout returns 500 for every customer in eu-west.`

![Is this a customer-facing outage?](docs/images/examples/ex-outage.png)

| p_outage |
| --- |
| 0.96 |

A later `WHERE p_outage > 0.8` keeps the pages worth waking someone for.

## Which team owns this email?

`email` is `Our SSO certificate expires Friday and login is already failing for new sessions.`

![Which team owns this email?](docs/images/examples/ex-inbox.png)

| label | confidence | probabilities |
| --- | --- | --- |
| security | 0.94 | security 0.94, billing 0.01, product 0.05 |

## How ready is this task for software?

`task` is `Every Friday I copy milestones from six boards into the same status update. The account lead approves it before it goes out.`

The rubric runs from 0, a person must decide, to 4, software can check its own work.

![How ready is this task for software?](docs/images/examples/ex-fit.png)

| fit | confidence | probabilities |
| --- | --- | --- |
| 3.10 | 0.81 | 0: 0.02, 1: 0.06, 2: 0.18, 3: 0.48, 4: 0.26 |

3.10 sits on "software can do it and a person approves," which matches a recurring draft that still needs a sign-off.

## What did the meeting actually decide?

`note` is `We agreed engineering will ship the checkout fix tomorrow. Support will email the affected accounts after it is live.`

![What did the meeting actually decide?](docs/images/examples/ex-note.png)

| decided | owner | owner confidence |
| --- | --- | --- |
| 0.93 | engineering | 0.88 |

`ask` returns the response text. The table is that text read back into columns. `owner` is engineering rather than support because the question asked who owns the next step, and the note names the fix first.
