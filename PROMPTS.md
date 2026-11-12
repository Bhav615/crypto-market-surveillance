# AI prompts used for project development

As required by the course rules, these are the prompts given to an AI code-generation tool
(Claude, by Anthropic) while developing this project.

---

## Prompt 1

> Final Project - Instructions
> - Each student should submit his/her own project. It is not a group-based project.
> - The project is to build a data streaming system to fulfill a business objective (Recommendation,
>   Fraud/Anomaly detection, CDC, etc.) in a business domain like Ecommerce, finance, energy, log, IoT, etc.
> - The data source must be an actual real-time data stream source like this example. Synthetic data
>   sources won't be accepted.
> - Like in any data pipeline, these measures matter: Throughput, Latency, Scalability, Reliability, Accuracy.
> - The data pipeline between the producers/consumers should include Kafka in the architecture.
> - The submission deadline is Friday 13.11.2026 at 12 PM.
> - You must show up on Friday 13.11.2026 at 12 PM to present your work.
> - The submission is done via the dedicated activity on e-campus. For that, either you submit your project
>   files/code (limited to 100 MB) directly on e-campus, or you share a link to your project repo in a text
>   file and The submission should include a README file. The file should include clear steps for how to
>   run/reproduce the project.
> - After you made the submission, download the submitted zip file and check that you have submitted
>   the correct files.
> - If a ChatGPT or a similar (code) generating tool is used for project development, include the
>   prompts in a separate file, as a text.
>
> these are the instructions given by professor so make a project according to this and topic should be
> like this in the link https://github.com/conduktor/public-streaming-api now i want a project like this
> definitely do not use synthetic data please follow everthing correctly or else i wont be graded

(A screenshot of the instruction slides was attached to this prompt.)

---

## Follow-up prompts (same session)

2. > so do i open this in vscode
3. > do this with kafka
4. > ok now i opened the folder in vscode next step
5. (screenshot of the Docker Desktop installer configuration dialog)
6. (screenshot: Docker installation failed – "Virtual Machine Platform" feature not enabled)
7. (screenshot: output of `wsl --install`)
8. (screenshot: `docker compose up` error "no configuration file provided")
9. (screenshot: Docker Desktop "Engine running")
10. (screenshot: `docker compose ps` output)
11. (screenshot: `docker compose ps` output, terminal maximized)
12. (screenshot: producer-coinbase logs)
13. (screenshots: producer-kraken / detector logs and `app.tools.report` output)
    > now check this and also tell me is this the whole project thats told by my professor and check
    > whether it covers all the required steps rules

   Result of prompt 13: added restart recovery to the producer (it reads the last published trade
   ids from Kafka on startup, so downtime gaps are detected and Coinbase gaps are backfilled), and
   added troubleshooting notes for clock skew.

14. > before that first explain me everything clearly from first step to last as my professor is very
    > strict he asks many questions about everything
15. > how to stop the running
16. > and how to stsrt the running
17. > now tell me evrytihng very clear and easily step by step of hwot the whole project is working and
    > what am i showing everything but once u tell a step then i will understand that ans say continue
    > they go to nwxt step
18. > before that i want to ask something that was also mentioned by my professor as must
    > Add 2 or 3 business objectives in the dash board
    > What if any server go off how will your project work
    > What if producer gets multiple data at once

   Result of prompt 18: dashboard rebuilt with 3 pages (Business objectives: whale surveillance,
   cross-exchange arbitrage, market risk; Market; Pipeline & servers with live Kafka broker/replica
   status and service heartbeats). README sections on server failures and burst handling.

## Further prompts

<!-- Add every further prompt you use here, in order. -->
