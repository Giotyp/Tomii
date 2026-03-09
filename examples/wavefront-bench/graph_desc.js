%%{init: {
  "theme": "base",
  "themeVariables": {
    "primaryColor":        "#d5e8d4",
    "primaryBorderColor":  "#82b366",
    "secondaryColor":      "#dae8fc",
    "secondaryBorderColor":"#6c8ebf",
    "edgeLabelBackground": "#ffffff",
    "fontSize":            "16px",
    "fontFamily":          "monospace"
  }
}}%%
flowchart TD
    classDef initNode fill:#dae8fc,stroke:#6c8ebf,stroke-width:2px,color:#000
    classDef diagNode fill:#d5e8d4,stroke:#82b366,stroke-width:1.5px,color:#000
    classDef peakNode fill:#ffe6cc,stroke:#d79b00,stroke-width:2.5px,color:#000

    GRID[("init_grid
    N×N f64
    boundary values")]:::initNode

    D0["diag₀   ×1"]:::diagNode
    D1["diag₁   ×2"]:::diagNode
    D2["diag₂   ×3"]:::diagNode
    D3["diag₃   ×N"]:::peakNode
    D4["diag₄   ×3"]:::diagNode
    D5["diag₅   ×2"]:::diagNode
    D6["diag₆   ×1"]:::diagNode

    GRID -. "$ref" .-> D0
    GRID -. "$ref" .-> D1
    GRID -. "$ref" .-> D2
    GRID -. "$ref" .-> D3
    GRID -. "$ref" .-> D4
    GRID -. "$ref" .-> D5
    GRID -. "$ref" .-> D6

    D0 -- "$barrier" --> D1
    D1 -- "$barrier" --> D2
    D2 -- "$barrier" --> D3
    D3 -- "$barrier" --> D4
    D4 -- "$barrier" --> D5
    D5 -- "$barrier" --> D6
