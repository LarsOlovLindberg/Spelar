# Inriktning: automatiserade satsningar (Polymarket + Kraken)

Det här projektet byter inriktning från en statisk “småspel”-sida till en sida/tjänst som hjälper till att upptäcka och genomföra **automatiserade satsningar** där man kombinerar:

- **Polymarket** (prediction markets, t.ex. “BTC når 100k innan datum X?”)
- **Kraken** derivat (primärt **options** för att prissätta hedge-kostnad, samt **futures/perps** för exekvering av hedge)

Målet är att identifiera “edge” i Polymarket genom att jämföra Polymarkets prissättning med vad det **kostar att säkra sig** via Kraken.

> Viktigt: Detta är en teknisk beskrivning av tänkt funktion. Det är inte finansiell rådgivning och inga resultat kan garanteras.

## Grundidé (edge via hedge-kostnad)

1. **Läs av marknad på Polymarket**
   - Hämta priset på “YES/NO” för ett event (ex: BTC >= 100k).
   - Tolka NO-priset som en implicit sannolikhet/utbetalningsprofil enligt Polymarkets struktur.

2. **Läs av hedge-kostnad på Kraken (options)**
   - Utgå från optionskedja/quotes för relevant underliggande (ex: BTC).
   - Räkna på vad det kostar att hedga uppsidan/nedsidan för ett scenario (ex: “BTC går till 100k”).
   - Optionspriser används som proxy för **marknadens kostnad för att försäkra**/säkra en position.

3. **Identifiera läge (“edge”/arb-liknande lägen)**
   - Jämför: 
     - förväntad payoff/odds från Polymarket (t.ex. NO)
     - mot hedge-kostnad och finansierings-/avgiftsantaganden från Kraken (options/futures)
   - Flagga situationer där skillnaden ser tillräckligt stor ut efter avgifter.

4. **Exekvera kombinerad position (Polymarket + Kraken futures)**
   - Placera satsning på Polymarket (ex: NO-bet).
   - Hedga på Kraken, typiskt med **futures/perps** (ex: long BTC) för att neutralisera scenario-risk.

## Exempel (konceptuellt)

- Event: “BTC når 100k”
- Polymarket: NO bet ger en viss payout om BTC *inte* når 100k.
- Kraken: beräkna vad det kostar att skydda sig om BTC ändå går mot/över 100k (options), och/eller skapa en dynamisk hedge via futures.
- Om Polymarkets NO-pris + hedge-kostnad (inkl avgifter) ger en attraktiv profil relativt risk, kan det vara ett “läge”.

## Alternativ edge: Polymarket "deadline ladder" (samma event, olika sluttid)

Det finns en särskild typ av intern Polymarket-edge när två marknader beskriver samma händelse men med olika deadline, t.ex.:

- A: "Kommer X hända *före* t1?" (tidigare sluttid)
- B: "Kommer X hända *före* t2?" där t2 > t1 (senare sluttid)

Logiken är monoton: om A inträffar så inträffar även B (A ⇒ B). Därför bör priset (implied p) uppfylla ungefär $p(A) \le p(B)$.

Om marknaden bryter monotoniciteten (praktiskt: A är dyrare än B) kan man ibland bygga en struktur som både hedgar och kan ge "dubbel utdelning":

- Köp (A) **NO**
- Köp (B) **YES**

Payoff (i tokens):

- X händer före t1: A_NO=0, B_YES=1 → total 1
- X händer mellan t1 och t2: A_NO=1, B_YES=1 → total 2 (dubbel win)
- X händer aldrig före t2: A_NO=1, B_YES=0 → total 1

Det är risk-minimerande eftersom du alltid får minst 1 i payoff; edge finns om inköpskostnaden är < 1 efter spread/fees.

I den här repot kan du scanna efter sådana par med snapshots i `web/data/pm_scan_candidates.csv` (orderbook bid/ask) och scriptet `scripts/find_pm_deadline_edges.py`.

## Datakällor och integrationer (planerad)

- Polymarket data:
  - Market metadata, YES/NO pris, likviditet, avgifter, event-datum.
- Kraken data:
  - Optionskedja/quotes (för prissättning av hedge-kostnad).
  - Futures/perps marknadsdata och exekvering (för hedging).

## Avgifter och friktion (måste modelleras)

För att “edge” ska vara verklig måste systemet ta hänsyn till friktion, minst:

- Polymarket: market fees/slippage/likviditet
- Kraken: trading fees, funding (för perps), spread/slippage
- (Ev.) capital constraints och exekveringslatens

## Sajtens fokus

Sajten/portalen är tänkt att fokusera på:

- Marknadsdata, beräkningar och signaler ("lägen")
- Integrationslogik mot Polymarket + Kraken
- Automatiserad/halvautomatiserad exekvering och riskhantering

## Nuvarande struktur (statisk portal)

- `web/index.html`: portal med router (hash-routes) som laddar innehåll från `web/pages/`.
- `web/pages/`: sidfragment (HTML) som injiceras i portalens content-yta.
- `web/data/`: datafiler (CSV/JSON) som kan laddas av sidor via `data-csv` / `data-json`.

## Nästa steg (när kod kommer)

- Definiera exakt vilka events/marknader som ska stödjas (t.ex. BTC-kring nivåer och datum).
- Specificera datamodell för:
  - Polymarket market snapshot
  - Kraken options snapshot
  - Hedge-kalkyl
  - Trade plan + exekveringsstatus
- Besluta runtime-miljö (ren statisk klient vs serverkomponent) baserat på behov av API-nycklar och säker exekvering.
