# AI prompts used for project development

As required by the course rules, these are the prompts given to an AI code-generation tool while developing this project.

---

## Prompt 1
> - The project is to build a data streaming system to fulfill a business objective (Recommendation,
>   Fraud/Anomaly detection, CDC, etc.) in a business domain like Ecommerce, finance, energy, log, IoT, etc.
> - The data source must be an actual real-time data stream source like this example. Synthetic data
>   sources won't be accepted.
> - Like in any data pipeline, these measures matter: Throughput, Latency, Scalability, Reliability, Accuracy.
> - The data pipeline between the producers/consumers should include Kafka in the architecture.
>   file and The submission should include a README file. The file should include clear steps for how to
> - If a ChatGPT or a similar (code) generating tool is used for project development, include the
>   prompts in a separate file, as a text.
> like this in the link https://github.com/conduktor/public-streaming-api now i want a project verified once
> definitely do not use synthetic data please follow everything correctly 

(A screenshot of the instruction slides was attached to this prompt.)

---

## Follow-up prompts (same session)
3. > do this work with kafka
4. > ok now i completed till validation in vscode next step
5. (screenshot of the Docker Desktop installer configuration dialog)
6. (screenshot: Docker installation failed – "Virtual Machine Platform" feature not enabled)
7. (screenshot: output of `wsl --install`)
8. (screenshot: `docker compose up` error "no configuration file provided")
9. (screenshot: Docker Desktop "Engine running")
10. (screenshot: `docker compose ps` output)
11. (screenshot: `docker compose ps` output, terminal maximized)
12. (screenshot: producer-coinbase logs)
13. (screenshots: producer-kraken / detector logs and `app.tools.report` output)
    > now check this and also tell me does this match the whole project objectives thats told by my professor and check
    > whether it covers all the required steps rules

   Result of prompt 13: added restart recovery to the producer (it reads the last published trade
   ids from Kafka on startup, so downtime gaps are detected and Coinbase gaps are backfilled), and
   added troubleshooting notes for clock skew.

    > what am i showing everything but once u tell a step then i will understand that ans say continue
    > they go to nwxt step
18. > before that i want to ask something that was also mentioned by my professor as must
    > What if any server go off how will your project work
    > What if producer gets multiple data at once

   Result of prompt 18: dashboard rebuilt with 3 pages (Business objectives: whale surveillance,
   cross-exchange arbitrage, market risk; Market; Pipeline & servers with live Kafka broker/replica
   status and service heartbeats). README sections on server failures and burst handling.

19. > instead of doing give everything as updated and new i will directly replace the old one
20. (screenshot: `docker compose up` "no configuration file provided" – wrong folder)
21. > its done what next
22. (screenshots: dashboard "Pipeline & servers" and "Business objectives" pages)
23. > my github username is Bhav615
24. (screenshots: GitHub repository before and after the push)
25. (output of `app.tools.report` after 4 days with the laptop switched off in between)

   Result of prompt 25: price-spike detector no longer compares prices across a long gap (laptop
   asleep / downtime); whale alerts record whether the trade was live or backfilled; the report
   now measures a time window (`--minutes`, default 60) instead of the whole history.

## Further prompts

<!-- Add every further prompt you use here, in order. -->
