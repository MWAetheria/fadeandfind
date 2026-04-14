import Stripe from "stripe";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

export default async function handler(req, res) {
  // CORS headers
  res.setHeader("Access-Control-Allow-Origin", "https://www.fadeandfind.com");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "Method not allowed" });

  const { priceId, userId, userEmail } = req.body;
  console.log("CHECKOUT BODY:", JSON.stringify({ priceId, userId, userEmail }));

  // Validate priceId is one of the two allowed values
  const allowedPrices = [
    process.env.STRIPE_MONTHLY_PRICE_ID,
    process.env.STRIPE_YEARLY_PRICE_ID,
  ];
  if (!allowedPrices.includes(priceId)) {
    return res.status(400).json({ error: "Invalid price ID" });
  }

  if (!userId || !userEmail) {
    return res.status(400).json({ error: "Missing userId or userEmail" });
  }

  try {
    const session = await stripe.checkout.sessions.create({
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: priceId, quantity: 1 }],
      customer_email: userEmail,
      client_reference_id: userId, // Supabase user ID — used in webhook
      success_url: "https://www.fadeandfind.com/?upgraded=true",
      cancel_url: "https://www.fadeandfind.com/?upgrade=cancelled",
      metadata: {
        supabase_user_id: userId,
      },
    });

    return res.status(200).json({ url: session.url });
  } catch (err) {
    console.error("Stripe checkout error:", err.message);
    return res.status(500).json({ error: "Failed to create checkout session" });
  }
}
