# CHRONOS-102

Find moving and changing infrared sources in NASA SPHEREx's 102 spectral bands, and check every candidate against Gaia, WISE/NEOWISE and JPL.

Made by ProximaZZZ Team. The user interface was designed with the help of Claude (Anthropic).

## Put it online (3 steps)

1. **Build the demo pack once.** Double-click `Build-Demo-Pack.pyw` (needs Python 3.9+; on macOS/Linux run `python3 Build-Demo-Pack.pyw`). It installs a few packages, opens the tool in your browser and downloads real archive data for the six demonstration targets into `website/demo/`. Keep the window open until it says "Demo pack ready". If a target fails, run it again; targets that were saved are kept.
2. **Upload the whole folder to a public GitHub repository.**
3. **Import the repository in Vercel.** No settings are needed: `vercel.json` publishes the `website` folder.

## What is in the folder

| Path | Purpose |
|---|---|
| `website/` | The published site: home page, `tool/` explorer, `demo/` saved data |
| `local-builder/` | Python helper used only by the builder (never published) |
| `Build-Demo-Pack.pyw` | One-click demo pack builder |
| `vercel.json`, `.vercelignore`, `.gitignore` | Hosting and repository settings |

To show your presentation or demo video on the home page, set the two links near the bottom of `website/index.html` (`window.CHRONOS_SITE`).

## Data sources

NASA SPHEREx (IRSA, DOI 10.26131/IRSA662), ESA Gaia DR3, NASA AllWISE / NEOWISE / CatWISE2020 (IRSA), NASA JPL Solar System Dynamics, CDS HiPS2FITS, NASA SkyView.

## Honest limits

- The default pixel view is a physics simulation built on real catalogue positions. Real SPHEREx exposures can be switched on and are saved for the six demonstration targets.
- Benchmark motions are drawn about 10x larger than reality so the dipoles are visible.
- The Planet X demo is schematic.
- The hosted site has no data server. Other positions work only if the visitor's browser can reach the archives directly. Flags made on the hosted site stay in that visitor's browser.
- Distances from proper motion alone assume a typical tangential velocity and are shown as ranges.
