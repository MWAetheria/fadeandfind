import Stripe from "stripe";
import { createClient } from "@supabase/supabase-js";

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Required: raw body for Stripe signature verification
export const config = {
  api: { bodyParser: false },
};

async function getRawBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

async function setUserPremium(userId, isPremium) {
  const { error } = await supabase
    .from("user_data")
    .update({ is_premium: isPremium })
    .eq("id", userId);

  if (error) {
    console.error(`Supabase update error for user ${userId}:`, error.message);
    throw error;
  }
  console.log(`User ${userId} is_premium set to ${isPremium}`);
}

export default async function handler(req, res) {
  if (req.method !== "POST") return res.status(405).end("Method not allowed");

  const sig = req.headers["stripe-signature"];
  const rawBody = await getRawBody(req);

  let event;
  try {
    event = stripe.webhooks.constructEvent(
      rawBody,
      sig,
      process.env.STRIPE_WEBHOOK_SECRET
    );
  } catch (err) {
    console.error("Webhook signature verification failed:", err.message);
    return res.status(400).send(`Webhook Error: ${err.message}`);
  }

  const { type, data } = event;
  const subscription = data.object;

  // Extract Supabase user ID from metadata (set at checkout)
  const userId =
    subscription.metadata?.supabase_user_id ||
    subscription.client_reference_id;

  if (!userId) {
    console.warn(`No userId found on event ${type} — skipping`);
    return res.status(200).json({ received: true });
  }

  try {
    switch (type) {
      case "customer.subscription.created":
      case "customer.subscription.updated": {
        // Active or trialing = premium
        const isActive = ["active", "trialing"].includes(subscription.status);
        await setUserPremium(userId, isActive);
        break;
      }

      case "customer.subscription.deleted": {
        await setUserPremium(userId, false);
        break;
      }

      default:
        console.log(`Unhandled event type: ${type}`);
    }
  } catch (err) {
    console.error(`Error handling event ${type}:`, err.message);
    return res.status(500).json({ error: "Internal server error" });
  }

  return res.status(200).json({ received: true });
}
