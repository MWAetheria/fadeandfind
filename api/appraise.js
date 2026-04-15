export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "https://www.fadeandfind.com");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { imageData, mediaType } = req.body;

  if (!imageData) return res.status(400).json({ error: "Missing image data" });

  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-opus-4-6",
        max_tokens: 1000,
        messages: [{
          role: "user",
          content: [
            {
              type: "image",
              source: {
                type: "base64",
                media_type: mediaType || "image/jpeg",
                data: imageData,
              }
            },
            {
              type: "text",
              text: `You are an expert antique appraiser and estate sale specialist. Analyze this item and provide:
1. Item identification (what it is, maker/brand if visible)
2. Estimated era/age
3. Style or category (e.g. Mid-Century Modern, Victorian, Art Deco)
4. Condition assessment based on what's visible
5. Estimated market value range (low to high in USD)
6. Where to sell it for the best price (e.g. eBay, local auction, specialty dealer, etc.)
7. Any notable features that affect value

Respond ONLY with a JSON object, no markdown, no preamble:
{
  "item": "string",
  "era": "string",
  "style": "string",
  "condition": "string",
  "value_low": number,
  "value_high": number,
  "best_to_sell": ["string"],
  "notes": "string"
}`
            }
          ]
        }]
      })
    });

    const data = await response.json();

    if (!response.ok) {
      console.error("Anthropic API error:", data);
      return res.status(500).json({ error: "AI analysis failed" });
    }

    const text = data.content?.[0]?.text || "";
    const clean = text.replace(/```json|```/g, "").trim();

    try {
      const parsed = JSON.parse(clean);
      return res.status(200).json(parsed);
    } catch {
      return res.status(500).json({ error: "Could not parse AI response" });
    }

  } catch (err) {
    console.error("Appraise error:", err.message);
    return res.status(500).json({ error: "Something went wrong" });
  }
}
