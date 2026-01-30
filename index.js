/**
 * Monetizelt Cloud Functions
 * Version: 2025-10-27
 * Changements clés:
 * - Expiration des produits à 7 jours
 * - Avertissement d’expiration à 24h restant (obligatoire)
 * - Cleanup supprime tout ce qui est lié au produit, sauf transactions/payouts
 * - Remplacement du checkout PayPal par Stripe Checkout
 * - Webhook Stripe pour fulfillment
 * - PayPal conservé pour les payouts
 * - Documentation détaillée
 */

/* ============================= IMPORTS & SECRETS ============================= */

const { defineSecret } = require("firebase-functions/params");
const sendgridApiKey = defineSecret("SENDGRID_API_KEY");
const paypalClientId = defineSecret("PAYPAL_CLIENT_ID");
const paypalClientSecret = defineSecret("PAYPAL_CLIENT_SECRET");
const stripeSecretKey = defineSecret("STRIPE_SECRET_KEY");
const stripeWebhookSecret = defineSecret("STRIPE_WEBHOOK_SECRET");

const { setGlobalOptions } = require("firebase-functions/v2");
const { onRequest } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const { onMessagePublished } = require("firebase-functions/v2/pubsub");

const admin = require("firebase-admin");
const paypalPayouts = require("@paypal/payouts-sdk");
const cors = require("cors");
const UAParser = require("ua-parser-js");
const crypto = require("crypto");
const rawBodySaver = require("raw-body");

/* ============================= GLOBAL CONFIG ============================= */

setGlobalOptions({
  region: "us-central1",
  memory: "256MiB",
});

admin.initializeApp({
  storageBucket: "monetizelt-b235d.firebasestorage.app",
});

const db = admin.firestore();
const bucket = admin.storage().bucket();

const corsMiddleware = cors({
  origin: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "Stripe-Signature"],
  credentials: true,
});

// Stripe client
let stripe = null;

/* ============================= CONSTANTES MÉTIER ============================= */

// Durée de vie produit: 7 jours
const PRODUCT_TTL_MS = 7 * 24 * 60 * 60 * 1000;

// Avertissement avant expiration: 24h avant la date d'expiration (inchangé)
const WARNING_BEFORE_EXP_MS = 24 * 60 * 60 * 1000;

// Durée par défaut des URLs signées read (alignée sur 7 jours)
const SIGNED_READ_URL_TTL_MS = 7 * 24 * 60 * 60 * 1000;

// Durée upload URL signée (inchangé)
const SIGNED_WRITE_URL_TTL_MS = 15 * 60 * 1000;

// Frais PayPal
const PAYPAL_FEE_RATE = 0.0349;
const PAYPAL_FEE_FIXED = 0.49;

// Commission plateforme
const PLATFORM_RATE = 0.12;

// Minimum payout
const MIN_PAYOUT = 10;

/* ============================= HELPERS GÉNÉRAUX ============================= */

function isEmail(s) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(s || "").toLowerCase());
}

function generateUniqueAccessToken(length = 32) {
  return crypto.randomBytes(length).toString("hex");
}

function extractDeviceInfo(userAgent) {
  const parser = new UAParser(userAgent || "");
  const r = parser.getResult();
  return {
    browser: `${r.browser.name || "Unknown"} ${r.browser.version || ""}`.trim(),
    os: `${r.os.name || "Unknown"} ${r.os.version || ""}`.trim(),
    device: r.device.type
      ? `${r.device.vendor || ""} ${r.device.model || ""} (${r.device.type})`.trim()
      : "Desktop",
    userAgent: userAgent || "",
  };
}

// Code à 6 chiffres
function generateVerificationCode() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

// Date d’expiration produit (48h)
function getProductExpirationDate(createdAtDate = new Date()) {
  return new Date(createdAtDate.getTime() + PRODUCT_TTL_MS);
}

// Enregistre un évènement de génération de lien (produit)
async function recordLinkGeneration(uid, productId) {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  const day = now.getDate();
  const hour = now.getHours();
  const minute = now.getMinutes();

  const hourId = `${year}-${month}-${day}-${hour}`;
  const minuteId = `${hourId}-${minute}`;

  const hourlyStatsRef = db.collection("linkGenerationStats").doc(hourId);
  await hourlyStatsRef.set(
    {
      count: admin.firestore.FieldValue.increment(1),
      year,
      month,
      day,
      hour,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  const minuteStatsRef = db.collection("linkGenerationStats").doc(minuteId);
  await minuteStatsRef.set(
    {
      count: admin.firestore.FieldValue.increment(1),
      year,
      month,
      day,
      hour,
      minute,
      lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  await db.collection("linkGenerationDetails").add({
    uid,
    productId,
    timestamp: admin.firestore.FieldValue.serverTimestamp(),
    year,
    month,
    day,
    hour,
    minute,
    expiresAt: admin.firestore.Timestamp.fromDate(getProductExpirationDate(now)),
  });
}

// Batch delete helper (max 500 docs)
async function deleteQueryBatch(query) {
  const snap = await query.get();
  if (snap.empty) return 0;
  const batch = db.batch();
  snap.docs.forEach((d) => batch.delete(d.ref));
  await batch.commit();
  return snap.size;
}

/* ============================= EMAIL NOTIFICATIONS ============================= */

async function sendEmailNotification(emailType, data) {
  try {
    const sgMail = require("@sendgrid/mail");
    sgMail.setApiKey(sendgridApiKey.value());

    const colors = {
      primary: "#007bff",
      secondary: "#20c997",
      accent: "#6f42c1",
      light: "#f8f9fa",
      dark: "#000000",
    };

    // Image profil: si tu as une URL statique propre, garde celle-ci
    const profileImageUrl =
      "https://firebasestorage.googleapis.com/v0/b/monetizelt-b235d.appspot.com/o/brand%2Favatar.png?alt=media";

    const emailHeader = `
      <div style="text-align:center;margin-bottom:15px;">
        <div style="display:inline-block;width:40px;height:40px;border-radius:50%;overflow:hidden;margin-bottom:5px;">
          <img src="${profileImageUrl}" alt="Profile" style="width:100%;height:100%;object-fit:cover;">
        </div>
        <p style="margin:5px 0 0 0;font-size:12px;color:#aaa;">Digital Content Marketplace</p>
      </div>
    `;

    let emailContent = null;

    switch (emailType) {
      case "sale_notification": {
        emailContent = {
          to: data.sellerEmail,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: `New Sale: ${data.productTitle}`,
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
              <h2 style="margin-top:0;font-size:16px;">New Sale!</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Your product has been purchased.</p>
              <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid ${colors.primary};">
                <p style="margin:5px 0;"><strong>Product:</strong> ${data.productTitle}</p>
                <p style="margin:5px 0;"><strong>Amount:</strong> $${Number(data.amount).toFixed(2)}</p>
                <p style="margin:5px 0;"><strong>Your Earnings:</strong> <span style="color:${colors.secondary};font-weight:bold;">$${Number(data.sellerAmount).toFixed(2)}</span></p>
              </div>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Weekly payouts every Friday for balances of $10 or more.</p>
              <div style="text-align:center;margin-top:15px;">
                <a href="https://www.g-z.online/dashboard.html" style="background:${colors.primary};color:white;padding:8px 16px;text-decoration:none;border-radius:10px;font-weight:bold;display:inline-block;font-size:14px;">View Dashboard</a>
              </div>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
      case "payout_notification": {
        const firstName = data.firstName || "Seller";

        // Optionnel: récupérer stats utilisateur pour enrichir l’email
        let userStats = { totalProducts: 0, totalOrders: 0, totalViews: 0 };
        try {
          const userStatsDoc = await db.collection("userStats").doc(data.userId || "").get();
          if (userStatsDoc.exists) {
            const s = userStatsDoc.data();
            userStats = {
              totalProducts: s.linksCount || 0,
              totalOrders: s.ordersCount || 0,
              totalViews: s.viewsCount || 0,
            };
          }
        } catch (e) {
          console.error("Error getting user stats for email:", e);
        }

        emailContent = {
          to: data.paypalEmail,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: "Your Monetizelt Payout Has Been Processed",
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
              <h2 style="margin-top:0;font-size:16px;">Hello ${firstName},</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">We've sent a payment to your PayPal account.</p>
              <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid ${colors.primary};">
                <p style="margin:5px 0;"><strong>Amount:</strong> <span style="color:${colors.secondary};font-weight:bold;">$${Number(data.amount).toFixed(2)}</span></p>
                <p style="margin:5px 0;"><strong>PayPal Email:</strong> ${data.paypalEmail}</p>
              </div>
              <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid ${colors.accent};">
                <p style="margin:5px 0;"><strong>Your Statistics:</strong></p>
                <p style="margin:5px 0;"><strong>Total Products Created:</strong> ${userStats.totalProducts}</p>
                <p style="margin:5px 0;"><strong>Total Orders:</strong> ${userStats.totalOrders}</p>
                <p style="margin:5px 0;"><strong>Total Views:</strong> ${userStats.totalViews}</p>
              </div>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">The funds should appear in your PayPal account shortly.</p>
              <div style="text-align:center;margin-top:15px;">
                <a href="https://www.g-z.online/dashboard.html" style="background:${colors.primary};color:white;padding:8px 16px;text-decoration:none;border-radius:10px;font-weight:bold;display:inline-block;font-size:14px;">View Dashboard</a>
              </div>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
      case "payout_failed": {
        emailContent = {
          to: data.paypalEmail,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: "Action Required: Problem with Your Payout",
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid #dc3545;">
              <h2 style="margin-top:0;font-size:16px;">Payment Issue</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">We encountered a problem when trying to send your payment.</p>
              <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid #dc3545;">
                <p style="margin:5px 0;"><strong>Amount:</strong> <span style="font-weight:bold;">$${Number(data.amount).toFixed(2)}</span></p>
                <p style="margin:5px 0;"><strong>PayPal Email:</strong> ${data.paypalEmail}</p>
                <p style="margin:5px 0;"><strong>Issue:</strong> ${data.error}</p>
              </div>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Please update your PayPal email to ensure you receive future payments.</p>
              <div style="text-align:center;margin-top:15px;">
                <a href="https://www.g-z.online/update-e-p.html" style="background:${colors.primary};color:white;padding:8px 16px;text-decoration:none;border-radius:10px;font-weight:bold;display:inline-block;font-size:14px;">Update PayPal Email</a>
              </div>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
      case "verification_code": {
        emailContent = {
          to: data.email,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: "Your Monetizelt Verification Code",
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
              <h2 style="margin-top:0;font-size:16px;">Email Verification</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Use the following verification code:</p>
              <div style="text-align:center;margin:20px 0;">
                <span style="font-size:24px;font-weight:bold;letter-spacing:3px;color:white;padding:10px 20px;background:#222;border-radius:10px;border-bottom:2px solid ${colors.primary};">${data.code}</span>
              </div>
              <p style="font-size:12px;color:#999;line-height:1.4;">This code will expire in 10 minutes.</p>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
      case "password_reset": {
        emailContent = {
          to: data.email,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: "Reset Your Monetizelt Password",
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
              <h2 style="margin-top:0;font-size:16px;">Password Reset</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Use this code to reset your password:</p>
              <div style="text-align:center;margin:20px 0;">
                <span style="font-size:24px;font-weight:bold;letter-spacing:3px;color:white;padding:10px 20px;background:#222;border-radius:10px;border-bottom:2px solid ${colors.primary};">${data.code}</span>
              </div>
              <p style="font-size:12px;color:#999;line-height:1.4;">This code will expire in 10 minutes.</p>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
      case "min_balance_not_reached": {
        emailContent = {
          to: data.email,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: "Payment Day: Balance Below Threshold",
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
              <h2 style="margin-top:0;font-size:16px;">Payment Day, However...</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Today is payment day, but your current balance is below the $${MIN_PAYOUT} minimum payout threshold.</p>
              <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid ${colors.primary};">
                <p style="margin:5px 0;"><strong>Your Current Balance:</strong> <span style="color:${colors.secondary};font-weight:bold;">$${Number(data.balance).toFixed(2)}</span></p>
                <p style="margin:5px 0;"><strong>Minimum Threshold:</strong> $${MIN_PAYOUT.toFixed(2)}</p>
              </div>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Keep selling! Your balance will carry over until you reach the minimum payout amount.</p>
              <div style="text-align:center;margin-top:15px;">
                <a href="https://www.g-z.online/dashboard.html" style="background:${colors.primary};color:white;padding:8px 16px;text-decoration:none;border-radius:10px;font-weight:bold;display:inline-block;font-size:14px;">View Dashboard</a>
              </div>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
      case "link_expiration_warning": {
        emailContent = {
          to: data.email,
          from: { email: "noreply@g-z.online", name: "Monetizelt" },
          subject: "Your Product Link Will Expire Soon",
          html: `
          <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:10px;">
            ${emailHeader}
            <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
              <h2 style="margin-top:0;font-size:16px;">Link Expiration Warning</h2>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">Your product link for "${data.productTitle}" will expire in ${data.hoursLeft} hours.</p>
              <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid ${colors.primary};">
                <p style="margin:5px 0;"><strong>Product:</strong> ${data.productTitle}</p>
                <p style="margin:5px 0;"><strong>Expires On:</strong> ${data.expirationDate}</p>
              </div>
              <p style="font-size:14px;color:#ddd;line-height:1.4;">After expiration, this link will no longer be accessible.</p>
              <div style="text-align:center;margin-top:15px;">
                <a href="https://www.g-z.online/dashboard.html" style="background:${colors.primary};color:white;padding:8px 16px;text-decoration:none;border-radius:10px;font-weight:bold;display:inline-block;font-size:14px;">View Dashboard</a>
              </div>
            </div>
            <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
          </div>`,
        };
        break;
      }
    }

    if (!emailContent) return false;
    await sgMail.send(emailContent);
    return true;
  } catch (error) {
    console.error("Error sending email notification:", error);
    return false;
  }
}

/* ============================= PAYOUTS HELPER ============================= */

async function processPayout(userId, amount, paypalEmail) {
  try {
    const payoutFee = PAYPAL_FEE_FIXED + amount * PAYPAL_FEE_RATE;
    const netAmount = Number(amount) - payoutFee;
    if (netAmount <= 0) throw new Error("Amount after fees is not positive");

    const environment =
      process.env.NODE_ENV === "production"
        ? new paypalPayouts.core.LiveEnvironment(paypalClientId.value(), paypalClientSecret.value())
        : new paypalPayouts.core.SandboxEnvironment(paypalClientId.value(), paypalClientSecret.value());

    const payoutsClient = new paypalPayouts.core.PayPalHttpClient(environment);

    if (!paypalPayouts.payouts || !paypalPayouts.payouts.PayoutsPostRequest) {
      throw new Error("PayPal Payouts SDK not correctly loaded");
    }

    const request = new paypalPayouts.payouts.PayoutsPostRequest();
    request.requestBody({
      sender_batch_header: {
        sender_batch_id: "batch_" + Date.now(),
        email_subject: "You have a payout!",
      },
      items: [
        {
          recipient_type: "EMAIL",
          amount: { value: netAmount.toFixed(2), currency: "USD" },
          receiver: paypalEmail,
          note: "Weekly payout",
          sender_item_id: userId,
        },
      ],
    });

    try {
      const resp = await payoutsClient.execute(request);

      // Débiter le solde utilisateur
      await db.collection("users").doc(userId).update({
        balance: admin.firestore.FieldValue.increment(-Number(amount)),
        lastPayout: admin.firestore.FieldValue.serverTimestamp(),
      });

      // Historique de payout
      await db.collection("payoutHistory").add({
        userId,
        amount: netAmount,
        grossAmount: Number(amount),
        payoutFee,
        paypalEmail,
        status: "completed",
        batchId: resp.result.batch_header.payout_batch_id,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });

      // Transaction payout
      await db.collection("transactions").add({
        userId,
        type: "payout",
        amount: netAmount,
        grossAmount: Number(amount),
        payoutFee,
        date: admin.firestore.FieldValue.serverTimestamp(),
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        status: "completed",
      });

      return resp;
    } catch (paypalError) {
      console.error("PayPal payout error:", paypalError);

      let errorType = "unknown";
      let errorMessage = "An unknown error occurred";

      const msg = paypalError.message || "";
      if (msg.toLowerCase().includes("insufficient")) {
        errorType = "insufficient_funds";
        errorMessage = "Insufficient funds in the PayPal account";
      } else if (msg.toLowerCase().includes("invalid") && msg.toLowerCase().includes("receiver")) {
        errorType = "invalid_paypal";
        errorMessage = "The PayPal email provided is invalid or not able to receive payments";
        await sendEmailNotification("payout_failed", {
          paypalEmail,
          amount: Number(amount),
          error: "Your PayPal email is not valid for receiving payments. Please update it in your dashboard.",
        });
      }

      await db.collection("payoutErrors").add({
        userId,
        amount: Number(amount),
        paypalEmail,
        errorType,
        errorMessage,
        originalError: msg || "No error message",
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });

      throw new Error(errorMessage);
    }
  } catch (err) {
    console.error(`Error in processPayout for ${userId}:`, err);
    throw err;
  }
}

/* ============================= USER STATS HELPER ============================= */

async function updateUserStats(uid, stats) {
  try {
    const userStatsRef = db.collection("userStats").doc(uid);
    const userStatsDoc = await userStatsRef.get();

    if (userStatsDoc.exists) {
      await userStatsRef.update({
        ...stats,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      });
    } else {
      await userStatsRef.set({
        linksCount: stats.linksCount || 0,
        viewsCount: stats.viewsCount || 0,
        ordersCount: stats.ordersCount || 0,
        shippedCount: stats.shippedCount || 0,
        revenueCount: stats.revenueCount || 0,
        lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
      });
    }
    return true;
  } catch (error) {
    console.error("Error updating user stats:", error);
    return false;
  }
}

/* ============================= SCHEDULED TASKS ============================= */

/**
 * Cleanup des produits expirés (48h) et de toutes les données liées,
 * tout en conservant les données financières (transactions, payoutHistory).
 * Fréquence: toutes les heures pour minimiser latence d’expiration.
 */
exports.cleanupOldProducts = onSchedule(
  {
    schedule: "0 * * * *", // toutes les heures
    timeZone: "UTC",
    memory: "256MiB",
    maxInstances: 1,
  },
  async () => {
    console.log("🧹 Starting hourly cleanup of expired products (7d TTL)");

    try {
      const now = new Date();
      const cutoffDate = new Date(now.getTime() - PRODUCT_TTL_MS);
      const cutoffTimestamp = admin.firestore.Timestamp.fromDate(cutoffDate);

      // Produits dont createdAt < now - 48h (expirés)
      const oldProductsSnap = await db.collection("products").where("createdAt", "<", cutoffTimestamp).get();
      console.log(`Found ${oldProductsSnap.size} products older than 7 days`);

      if (oldProductsSnap.empty) return null;

      const productsToDelete = oldProductsSnap.docs.map((doc) => ({ id: doc.id, ...doc.data() }));

      await db.collection("productCleanupLogs").add({
        date: admin.firestore.FieldValue.serverTimestamp(),
        count: oldProductsSnap.size,
        productIds: productsToDelete.map((p) => p.id),
      });

      const sellerImpact = {};

      for (const product of productsToDelete) {
        const productId = product.id;
        const sellerUid = product.uid;

        if (sellerUid) {
          sellerImpact[sellerUid] = sellerImpact[sellerUid] || {
            productsDeleted: 0,
          };
          sellerImpact[sellerUid].productsDeleted += 1;
        }

        // 1) Delete Storage files
        if (product.coverPath) {
          try {
            await bucket.file(product.coverPath).delete();
            console.log(`Deleted cover file: ${product.coverPath}`);
          } catch (e) {
            console.error(`Error deleting cover file ${product.coverPath}:`, e.message);
          }
        }
        if (product.filePath) {
          try {
            await bucket.file(product.filePath).delete();
            console.log(`Deleted product file: ${product.filePath}`);
          } catch (e) {
            console.error(`Error deleting product file ${product.filePath}:`, e.message);
          }
        }

        // 2) Views
        try {
          await deleteQueryBatch(db.collection("productViews").where("productId", "==", productId));
        } catch (e) {
          console.error(`Error deleting productViews for ${productId}:`, e.message);
        }

        // 3) Orders + access logs/attempts (ATTENTION: on supprime les orders,
        // mais on NE SUPPRIME PAS les transactions/payoutHistory.)
        let ordersSnap;
        try {
          ordersSnap = await db.collection("orders").where("productId", "==", productId).get();
        } catch (e) {
          console.error(`Error querying orders for ${productId}:`, e.message);
          ordersSnap = { empty: true, docs: [], size: 0 };
        }

        if (!ordersSnap.empty) {
          for (const orderDoc of ordersSnap.docs) {
            const orderId = orderDoc.id;
            try {
              await deleteQueryBatch(db.collection("accessLogs").where("orderId", "==", orderId));
            } catch (e) {
              console.error(`Error deleting accessLogs for order ${orderId}:`, e.message);
            }
            try {
              await deleteQueryBatch(db.collection("accessAttempts").where("orderId", "==", orderId));
            } catch (e) {
              console.error(`Error deleting accessAttempts for order ${orderId}:`, e.message);
            }
          }
          try {
            const batch = db.batch();
            ordersSnap.docs.forEach((d) => batch.delete(d.ref));
            await batch.commit();
          } catch (e) {
            console.error(`Error deleting orders for product ${productId}:`, e.message);
          }
        }

        // 4) Payment sessions
        try {
          await deleteQueryBatch(db.collection("paymentSessions").where("productId", "==", productId));
        } catch (e) {
          console.error(`Error deleting paymentSessions for ${productId}:`, e.message);
        }

        // 5) Emails logs
        try {
          await deleteQueryBatch(db.collection("emailSentLogs").where("productId", "==", productId));
        } catch (e) {
          console.error(`Error deleting emailSentLogs for ${productId}:`, e.message);
        }

        // 6) Link generation detail
        try {
          await deleteQueryBatch(db.collection("linkGenerationDetails").where("productId", "==", productId));
        } catch (e) {
          console.error(`Error deleting linkGenerationDetails for ${productId}:`, e.message);
        }

        // 7) Product doc
        try {
          await db.collection("products").doc(productId).delete();
        } catch (e) {
          console.error(`Error deleting product doc ${productId}:`, e.message);
        }
      }

      // Recalcule rapide des stats des vendeurs impactés
      for (const uid of Object.keys(sellerImpact)) {
        try {
          const [remainingProductsSnap, viewsSnap, ordersSnap] = await Promise.all([
            db.collection("products").where("uid", "==", uid).get(),
            db.collection("productViews").where("sellerUid", "==", uid).get(),
            db.collection("orders").where("sellerUid", "==", uid).get(),
          ]);

          const linksCount = remainingProductsSnap.size;
          const viewsCount = viewsSnap.size;

          const orders = ordersSnap.docs.map((d) => d.data());
          const ordersCount = orders.length;
          const shippedCount = orders.filter((o) => o.status === "shipped" || o.status === "delivered").length;
          const revenueCount = orders.reduce((sum, o) => {
            if (o.sellerAmount && o.status !== "cancelled") return sum + Number(o.sellerAmount || 0);
            return sum;
          }, 0);

          await db.collection("userStats").doc(uid).set(
            {
              linksCount,
              viewsCount,
              ordersCount,
              shippedCount,
              revenueCount,
              lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          );
        } catch (e) {
          console.error(`Error updating userStats for ${uid}:`, e.message);
        }
      }

      console.log(`Successfully cleaned ${productsToDelete.length} expired products and related data`);
      return null;
    } catch (error) {
      console.error("Error cleaning up old products:", error);
      await db.collection("systemErrors").add({
        function: "cleanupOldProducts",
        error: error.message || "Unknown error",
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        stack: error.stack,
      });
      return null;
    }
  }
);

/**
 * Avertissements d’expiration: 24h avant l’expiration.
 */
exports.sendExpirationWarnings = onSchedule(
  {
    schedule: "0 * * * *", // toutes les heures
    timeZone: "UTC",
    memory: "256MiB",
    maxInstances: 1,
  },
  async () => {
    console.log("🔔 Checking for 24h-to-expire products (hourly)");

    try {
      const now = new Date();
      const windowStart = new Date(now.getTime() + WARNING_BEFORE_EXP_MS);
      const windowEnd = new Date(windowStart.getTime() + 60 * 60 * 1000); // fenêtre 1h

      const productsSnap = await db
        .collection("products")
        .where("expiresAt", ">=", admin.firestore.Timestamp.fromDate(windowStart))
        .where("expiresAt", "<", admin.firestore.Timestamp.fromDate(windowEnd))
        .get();

      console.log(`Found ${productsSnap.size} products expiring in ~24h`);

      let emailsSent = 0;

      for (const doc of productsSnap.docs) {
        const product = doc.data();
        const productId = doc.id;

        const existingWarningSnap = await db
          .collection("emailSentLogs")
          .where("type", "==", "expiration_warning")
          .where("productId", "==", productId)
          .limit(1)
          .get();

        if (!existingWarningSnap.empty) {
          continue;
        }

        const sellerDoc = await db.collection("users").doc(product.uid).get();
        if (!sellerDoc.exists) continue;

        const sellerEmail = sellerDoc.data().email;
        if (!sellerEmail || !isEmail(sellerEmail)) continue;

        const expirationDate = product.expiresAt.toDate();
        const formattedDate = expirationDate.toLocaleString("en-US", {
          year: "numeric",
          month: "long",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
        });

        const ok = await sendEmailNotification("link_expiration_warning", {
          email: sellerEmail,
          productTitle: product.title || "Your product",
          expirationDate: formattedDate,
          hoursLeft: 24,
        });

        if (ok) {
          emailsSent++;
          await db.collection("emailSentLogs").add({
            type: "expiration_warning",
            productId: productId,
            sellerUid: product.uid,
            email: sellerEmail,
            sentAt: admin.firestore.FieldValue.serverTimestamp(),
            expiresAt: product.expiresAt,
          });
        }

        await new Promise((r) => setTimeout(r, 250));
      }

      console.log(`Expiration warnings sent: ${emailsSent}`);
      return null;
    } catch (error) {
      console.error("Error sending expiration warnings:", error);
      await db.collection("systemErrors").add({
        function: "sendExpirationWarnings",
        error: error.message || "Unknown error",
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        stack: error.stack,
      });
      return null;
    }
  }
);

/**
 * Paiements hebdomadaires (vendredi) - via PayPal Payouts (conservé)
 */
exports.processWeeklyPayouts = onSchedule(
  {
    schedule: "0 12 * * 5", // Vendredi 12:00 UTC
    timeZone: "UTC",
    memory: "256MiB",
    maxInstances: 1,
  },
  async () => {
    console.log("💰 Starting weekly payouts process");

    try {
      const payoutSessionRef = await db.collection("payoutSessions").add({
        startTime: admin.firestore.FieldValue.serverTimestamp(),
        status: "started",
        processedUsers: 0,
        successfulPayouts: 0,
        failedPayouts: 0,
        totalAmount: 0,
      });

      const usersSnap = await db
        .collection("users")
        .where("balance", ">=", MIN_PAYOUT)
        .where("paypalEmail", "!=", null)
        .get();

      let processedUsers = 0;
      let successfulPayouts = 0;
      let failedPayouts = 0;
      let totalAmount = 0;

      for (const userDoc of usersSnap.docs) {
        const user = userDoc.data();
        const userId = userDoc.id;
        const balance = Number(user.balance || 0);
        const paypalEmail = user.paypalEmail;

        if (!isEmail(paypalEmail)) {
          failedPayouts++;
          continue;
        }
        if (balance < MIN_PAYOUT) continue;

        processedUsers++;

        try {
          const profile = await db.collection("users").doc(userId).get();
          const firstName =
            (profile.exists && (profile.data().firstName || profile.data().displayName || "Seller")) || "Seller";

          await processPayout(userId, balance, paypalEmail);

          await sendEmailNotification("payout_notification", {
            userId,
            paypalEmail,
            amount: balance - (PAYPAL_FEE_FIXED + balance * PAYPAL_FEE_RATE),
            firstName,
          });

          successfulPayouts++;
          totalAmount += balance;
        } catch (error) {
          console.error(`Error processing payout for ${userId}:`, error);
          await db.collection("payoutErrors").add({
            userId,
            amount: balance,
            paypalEmail,
            error: error.message || "Unknown error",
            timestamp: admin.firestore.FieldValue.serverTimestamp(),
          });
          failedPayouts++;
        }

        await new Promise((r) => setTimeout(r, 1000));
      }

      const lowBalanceUsersSnap = await db
        .collection("users")
        .where("balance", ">", 0)
        .where("balance", "<", MIN_PAYOUT)
        .where("paypalEmail", "!=", null)
        .get();

      for (const userDoc of lowBalanceUsersSnap.docs) {
        const user = userDoc.data();
        const email = user.email;
        if (email && isEmail(email)) {
          await sendEmailNotification("min_balance_not_reached", { email, balance: Number(user.balance || 0) });
        }
      }

      await payoutSessionRef.update({
        endTime: admin.firestore.FieldValue.serverTimestamp(),
        status: "completed",
        processedUsers,
        successfulPayouts,
        failedPayouts,
        totalAmount,
      });

      console.log(
        `Weekly payout completed: ${successfulPayouts} success, ${failedPayouts} failed, $${totalAmount.toFixed(
          2
        )} total`
      );
      return null;
    } catch (error) {
      console.error("Error processing weekly payouts:", error);
      await db.collection("systemErrors").add({
        function: "processWeeklyPayouts",
        error: error.message || "Unknown error",
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
        stack: error.stack,
      });
      return null;
    }
  }
);

/* ============================= HTTP FUNCTIONS ============================= */
/* Note: checkout = Stripe désormais. Payout = PayPal maintenu. */

/* ---- Email Verification ---- */

exports.sendVerificationCode = onRequest({ secrets: [sendgridApiKey] }, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const { email, code } = req.body || {};
      if (!email || !code) return res.status(400).json({ success: false, error: "Missing email or code" });
      if (!isEmail(email)) return res.status(400).json({ success: false, error: "Invalid email format" });

      const ok = await sendEmailNotification("verification_code", { email, code });
      if (!ok) return res.status(500).json({ success: false, error: "Failed to send verification code" });

      await db.collection("verificationCodes").add({
        email,
        code,
        type: "account_verification",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        expiresAt: new Date(Date.now() + 10 * 60 * 1000),
      });

      return res.status(200).json({ success: true, message: "Verification code sent successfully" });
    } catch (err) {
      console.error("sendVerificationCode error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.sendPasswordResetCode = onRequest({ secrets: [sendgridApiKey] }, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const { email } = req.body || {};
      if (!email) return res.status(400).json({ success: false, error: "Missing email" });
      if (!isEmail(email)) return res.status(400).json({ success: false, error: "Invalid email format" });

      const code = generateVerificationCode();

      const old = await db
        .collection("verificationCodes")
        .where("email", "==", email)
        .where("type", "==", "password_reset")
        .get();
      const batch = db.batch();
      old.docs.forEach((d) => batch.delete(d.ref));
      await batch.commit();

      const ok = await sendEmailNotification("password_reset", { email, code });
      if (!ok) return res.status(500).json({ success: false, error: "Failed to send password reset code" });

      await db.collection("verificationCodes").add({
        email,
        code,
        type: "password_reset",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        expiresAt: new Date(Date.now() + 10 * 60 * 1000),
      });

      return res.status(200).json({ success: true, message: "Password reset code sent successfully" });
    } catch (err) {
      console.error("sendPasswordResetCode error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.verifyCode = onRequest({}, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const { email, code, type } = req.body || {};
      if (!email || !code || !type) return res.status(400).json({ success: false, error: "Missing fields" });
      if (!isEmail(email)) return res.status(400).json({ success: false, error: "Invalid email format" });

      const codesSnap = await db
        .collection("verificationCodes")
        .where("email", "==", email)
        .where("type", "==", type)
        .where("expiresAt", ">", new Date())
        .orderBy("expiresAt", "desc")
        .limit(1)
        .get();

      if (codesSnap.empty) return res.status(400).json({ success: false, error: "No valid verification code found" });

      const data = codesSnap.docs[0].data();
      if (data.code !== code) return res.status(400).json({ success: false, error: "Invalid verification code" });

      await codesSnap.docs[0].ref.update({
        used: true,
        usedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      return res.status(200).json({ success: true, message: "Code verified successfully" });
    } catch (err) {
      console.error("verifyCode error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.resetPassword = onRequest({}, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const { email, newPassword, code } = req.body || {};
      if (!email || !newPassword || !code) return res.status(400).json({ success: false, error: "Missing fields" });
      if (!isEmail(email)) return res.status(400).json({ success: false, error: "Invalid email format" });
      if (newPassword.length < 6) return res.status(400).json({ success: false, error: "Password too short" });

      const codesSnap = await db
        .collection("verificationCodes")
        .where("email", "==", email)
        .where("type", "==", "password_reset")
        .where("expiresAt", ">", new Date())
        .orderBy("expiresAt", "desc")
        .limit(1)
        .get();

      if (codesSnap.empty) return res.status(400).json({ success: false, error: "No valid verification code found" });

      if (codesSnap.docs[0].data().code !== code)
        return res.status(400).json({ success: false, error: "Invalid verification code" });

      const usersSnap = await db.collection("users").where("email", "==", email).limit(1).get();
      if (usersSnap.empty) return res.status(404).json({ success: false, error: "User not found" });

      try {
        const userRecord = await admin.auth().getUserByEmail(email);
        await admin.auth().updateUser(userRecord.uid, { password: newPassword });

        await codesSnap.docs[0].ref.update({
          used: true,
          usedAt: admin.firestore.FieldValue.serverTimestamp(),
        });

        await db.collection("passwordResetLogs").add({
          email,
          userId: userRecord.uid,
          timestamp: admin.firestore.FieldValue.serverTimestamp(),
          success: true,
        });

        return res.status(200).json({ success: true, message: "Password reset successfully" });
      } catch (authError) {
        console.error("Error updating Firebase Auth user:", authError);
        return res.status(500).json({ success: false, error: "Failed to update password in auth system" });
      }
    } catch (err) {
      console.error("resetPassword error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/* ---- Stats & Data ---- */

exports.getUserStats = onRequest({}, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const userStatsDoc = await db.collection("userStats").doc(uid).get();
      if (!userStatsDoc.exists) {
        await db
          .collection("userStats")
          .doc(uid)
          .set({
            linksCount: 0,
            viewsCount: 0,
            ordersCount: 0,
            shippedCount: 0,
            revenueCount: 0,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          });
        return res.status(200).json({
          success: true,
          stats: { linksCount: 0, viewsCount: 0, ordersCount: 0, shippedCount: 0, revenueCount: 0 },
        });
      }

      const s = userStatsDoc.data();
      return res.status(200).json({
        success: true,
        stats: {
          linksCount: s.linksCount || 0,
          viewsCount: s.viewsCount || 0,
          ordersCount: s.ordersCount || 0,
          shippedCount: s.shippedCount || 0,
          revenueCount: s.revenueCount || 0,
        },
      });
    } catch (err) {
      console.error("getUserStats error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.recordProductView = onRequest({}, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const { productId } = req.body || {};
      if (!productId) return res.status(400).json({ success: false, error: "Missing productId" });

      const productDoc = await db.collection("products").doc(productId).get();
      if (!productDoc.exists) return res.status(404).json({ success: false, error: "Product not found" });

      const product = productDoc.data();
      const sellerUid = product.uid;

      await db.collection("productViews").add({
        productId,
        sellerUid,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });

      const userStatsRef = db.collection("userStats").doc(sellerUid);
      const userStatsDoc = await userStatsRef.get();
      if (userStatsDoc.exists) {
        await userStatsRef.update({
          viewsCount: admin.firestore.FieldValue.increment(1),
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        });
      } else {
        await userStatsRef.set({
          linksCount: 0,
          viewsCount: 1,
          ordersCount: 0,
          shippedCount: 0,
          revenueCount: 0,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        });
      }

      return res.status(200).json({ success: true });
    } catch (err) {
      console.error("recordProductView error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getTransactionHistory = onRequest({}, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const transactionsSnap = await db
        .collection("transactions")
        .where("userId", "==", uid)
        .orderBy("createdAt", "desc")
        .get();

      const transactions = transactionsSnap.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
        date: doc.data().date ? doc.data().date.toDate() : null,
      }));

      const payoutsSnap = await db.collection("payoutHistory").where("userId", "==", uid).orderBy("timestamp", "desc").get();
      const payouts = payoutsSnap.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
        timestamp: doc.data().timestamp ? doc.data().timestamp.toDate() : null,
        type: "payout",
      }));

      const all = [...transactions, ...payouts].sort((a, b) => {
        const da = a.date || a.timestamp || 0;
        const dbb = b.date || b.timestamp || 0;
        return dbb - da;
      });

      return res.status(200).json({ success: true, transactions: all });
    } catch (err) {
      console.error("getTransactionHistory error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getLinkGenerationStats = onRequest({}, async (req, res) => {
  return corsMiddleware(req, res, async () => {
    try {
      const { period, uid } = req.query;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      let statsQuery;
      if (period === "hourly") {
        const since = new Date(Date.now() - 24 * 60 * 60 * 1000);
        statsQuery = db.collection("linkGenerationStats").where("lastUpdated", ">", since).orderBy("lastUpdated", "asc");
      } else if (period === "daily") {
        const since = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
        statsQuery = db.collection("linkGenerationStats").where("lastUpdated", ">", since).orderBy("lastUpdated", "asc");
      } else {
        statsQuery = db.collection("linkGenerationStats").orderBy("lastUpdated", "desc").limit(100);
      }

      const statsSnap = await statsQuery.get();

      const detailsSnap = await db
        .collection("linkGenerationDetails")
        .where("uid", "==", uid)
        .orderBy("timestamp", "desc")
        .limit(100)
        .get();

      const userLinks = detailsSnap.docs.map((d) => ({
        id: d.id,
        ...d.data(),
        timestamp: d.data().timestamp ? d.data().timestamp.toDate() : null,
        expiresAt: d.data().expiresAt ? d.data().expiresAt.toDate() : null,
      }));

      const stats = statsSnap.docs.map((d) => ({
        id: d.id,
        ...d.data(),
        lastUpdated: d.data().lastUpdated ? d.data().lastUpdated.toDate() : null,
      }));

      return res.status(200).json({ success: true, stats, userLinks });
    } catch (err) {
      console.error("getLinkGenerationStats error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/* ---- Produits & Paiements ---- */

exports.createProduct = onRequest({ secrets: [sendgridApiKey] }, async (req, res) => {
  // Preflight simple
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const { uid, title, category, description, price, fileName } = req.body || {};
      if (!uid || !title || !price || !fileName)
        return res.status(400).json({ success: false, error: "Missing required fields" });

      if (parseFloat(price) < 1) return res.status(400).json({ success: false, error: "Minimum price is $1.00" });

      const validCategories = ["video", "ebook", "music", "software"];
      if (!category || !validCategories.includes(category))
        return res.status(400).json({ success: false, error: "Invalid category" });

      const now = Date.now();
      const coverPath = `covers/${uid}/${now}_cover_${fileName}`;
      const filePath = `products/${uid}/${now}_${fileName}`;

      const coverFile = bucket.file(coverPath);
      const mainFile = bucket.file(filePath);

      const [coverUrl] = await coverFile.getSignedUrl({
        version: "v4",
        action: "write",
        expires: Date.now() + SIGNED_WRITE_URL_TTL_MS,
        contentType: "application/octet-stream",
        extensionHeaders: { "x-goog-content-length-range": "0,10485760" }, // 10MB
      });

      let sizeLimit = "0,104857600"; // 100MB
      if (category === "video") sizeLimit = "0,524288000"; // 500MB

      const [fileUrl] = await mainFile.getSignedUrl({
        version: "v4",
        action: "write",
        expires: Date.now() + SIGNED_WRITE_URL_TTL_MS,
        contentType: "application/octet-stream",
        extensionHeaders: { "x-goog-content-length-range": sizeLimit },
      });

      // Expiration 48h
      const expirationDate = getProductExpirationDate(new Date());

      const productDoc = {
        uid,
        title,
        category,
        description: description || "",
        price: parseFloat(price),
        coverPath,
        filePath,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        expiresAt: admin.firestore.Timestamp.fromDate(expirationDate),
        sales: 0,
        revenue: 0,
        filesUploaded: false,
      };

      const productRef = await db.collection("products").add(productDoc);

      await productRef.update({
        shareableLink: `https://www.g-z.online/product.html?productId=${productRef.id}`,
      });

      // Stats utilisateur
      const productsSnap = await db.collection("products").where("uid", "==", uid).get();
      const linksCount = productsSnap.size;
      await updateUserStats(uid, { linksCount });

      await recordLinkGeneration(uid, productRef.id);

      return res.status(200).json({
        success: true,
        productId: productRef.id,
        uploadUrls: { cover: coverUrl, file: fileUrl },
        shareableLink: `https://www.g-z.online/product.html?productId=${productRef.id}`,
        expiresAt: expirationDate,
        stats: { linksCount },
      });
    } catch (err) {
      console.error("createProduct error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getUserPaypalStatus = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const snap = await db.collection("users").doc(uid).get();
      if (!snap.exists)
        return res
          .status(200)
          .json({ success: true, exists: false, onboardingComplete: false, paypalEmail: null, balance: 0 });

      const u = snap.data();
      return res.status(200).json({
        success: true,
        exists: true,
        onboardingComplete: Boolean(u.onboardingComplete) && Boolean(u.paypalEmail),
        paypalEmail: u.paypalEmail || null,
        balance: Number(u.balance || 0),
        lastPayout: u.lastPayout || null,
      });
    } catch (err) {
      console.error("getUserPaypalStatus error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.updatePaypalEmail = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const { uid, paypalEmail } = req.body || {};
      if (!uid || !paypalEmail) return res.status(400).json({ success: false, error: "Missing uid or email" });
      if (!isEmail(paypalEmail)) return res.status(400).json({ success: false, error: "Invalid email" });

      await db
        .collection("users")
        .doc(uid)
        .set(
          { paypalEmail, onboardingComplete: true, lastUpdate: admin.firestore.FieldValue.serverTimestamp() },
          { merge: true }
        );

      return res.status(200).json({ success: true });
    } catch (err) {
      console.error("updatePaypalEmail error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getUserData = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const doc = await db.collection("users").doc(uid).get();
      if (!doc.exists) return res.status(404).json({ success: false, error: "User not found" });

      let userData = doc.data();

      const userStatsDoc = await db.collection("userStats").doc(uid).get();
      if (userStatsDoc.exists) {
        const s = userStatsDoc.data();
        userData = {
          ...userData,
          linksCount: s.linksCount || 0,
          viewsCount: s.viewsCount || 0,
          ordersCount: s.ordersCount || 0,
          shippedCount: s.shippedCount || 0,
          revenueCount: s.revenueCount || 0,
        };
      } else {
        userData = { ...userData, linksCount: 0, viewsCount: 0, ordersCount: 0, shippedCount: 0, revenueCount: 0 };
        await db.collection("userStats").doc(uid).set({
          linksCount: 0,
          viewsCount: 0,
          ordersCount: 0,
          shippedCount: 0,
          revenueCount: 0,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        });
      }

      return res.status(200).json({ success: true, data: userData });
    } catch (err) {
      console.error("getUserData error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getUserName = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const doc = await db.collection("users").doc(uid).get();
      if (!doc.exists) return res.status(404).json({ success: false, error: "User not found" });

      const user = doc.data();
      const displayName = user.displayName || user.firstName || "User";
      const firstName = displayName.split(" ")[0];

      return res.status(200).json({ success: true, firstName, displayName });
    } catch (err) {
      console.error("getUserName error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/* ---- Delete Account & Product ---- */

exports.deleteUserAccount = onRequest({ region: "us-central1", memory: "256MiB" }, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const { uid } = req.body || {};
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const userDoc = await db.collection("users").doc(uid).get();
      if (!userDoc.exists) return res.status(404).json({ success: false, error: "User not found" });

      const productsSnap = await db.collection("products").where("uid", "==", uid).get();
      const productIds = productsSnap.docs.map((d) => d.id);

      // Delete Storage files
      for (const doc of productsSnap.docs) {
        const product = doc.data();
        if (product.coverPath) {
          try {
            await bucket.file(product.coverPath).delete();
          } catch (e) {
            console.error(`Error deleting cover file ${product.coverPath}:`, e);
          }
        }
        if (product.filePath) {
          try {
            await bucket.file(product.filePath).delete();
          } catch (e) {
            console.error(`Error deleting product file ${product.filePath}:`, e);
          }
        }
      }

      // Delete related collections for each product
      for (const pid of productIds) {
        try {
          await deleteQueryBatch(db.collection("productViews").where("productId", "==", pid));
          const ordersSnap = await db.collection("orders").where("productId", "==", pid).get();
          for (const o of ordersSnap.docs) {
            const orderId = o.id;
            await deleteQueryBatch(db.collection("accessLogs").where("orderId", "==", orderId));
            await deleteQueryBatch(db.collection("accessAttempts").where("orderId", "==", orderId));
          }
          const batch = db.batch();
          ordersSnap.docs.forEach((d) => batch.delete(d.ref));
          await batch.commit();

          await deleteQueryBatch(db.collection("paymentSessions").where("productId", "==", pid));
          await deleteQueryBatch(db.collection("emailSentLogs").where("productId", "==", pid));
          await deleteQueryBatch(db.collection("linkGenerationDetails").where("productId", "==", pid));
        } catch (e) {
          console.error(`Error cascading deletes for product ${pid}:`, e.message);
        }
      }

      // Delete products
      const batchDel = db.batch();
      productsSnap.docs.forEach((d) => batchDel.delete(d.ref));

      // Log account deletion
      batchDel.set(db.collection("deletedAccounts").doc(), {
        uid,
        email: userDoc.data().email,
        displayName: userDoc.data().displayName || "Unknown",
        deletedAt: admin.firestore.FieldValue.serverTimestamp(),
        productsDeleted: productIds.length,
        productIds,
      });

      // Delete stats and user doc (ne pas toucher aux transactions/payoutHistory)
      batchDel.delete(db.collection("userStats").doc(uid));
      batchDel.delete(db.collection("users").doc(uid));

      await batchDel.commit();

      return res.status(200).json({ success: true, message: "Account and associated data deleted successfully" });
    } catch (err) {
      console.error("deleteUserAccount error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.deleteProduct = onRequest({ region: "us-central1", memory: "256MiB" }, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const { uid, productId } = req.body || {};
      if (!uid || !productId) return res.status(400).json({ success: false, error: "Missing uid or productId" });

      const productDoc = await db.collection("products").doc(productId).get();
      if (!productDoc.exists) return res.status(404).json({ success: false, error: "Product not found" });

      const product = productDoc.data();
      if (product.uid !== uid) return res.status(403).json({ success: false, error: "Forbidden" });

      // Files
      if (product.coverPath) {
        try {
          await bucket.file(product.coverPath).delete();
        } catch (e) {
          console.error(`Error deleting cover file ${product.coverPath}:`, e.message);
        }
      }
      if (product.filePath) {
        try {
          await bucket.file(product.filePath).delete();
        } catch (e) {
          console.error(`Error deleting product file ${product.filePath}:`, e.message);
        }
      }

      // Views
      await deleteQueryBatch(db.collection("productViews").where("productId", "==", productId));

      // Orders and access logs/attempts (do not delete transactions)
      const ordersSnap = await db.collection("orders").where("productId", "==", productId).get();
      if (!ordersSnap.empty) {
        for (const orderDoc of ordersSnap.docs) {
          const orderId = orderDoc.id;
          await deleteQueryBatch(db.collection("accessLogs").where("orderId", "==", orderId));
          await deleteQueryBatch(db.collection("accessAttempts").where("orderId", "==", orderId));
        }
        const batch = db.batch();
        ordersSnap.docs.forEach((d) => batch.delete(d.ref));
        await batch.commit();
      }

      await deleteQueryBatch(db.collection("paymentSessions").where("productId", "==", productId));
      await deleteQueryBatch(db.collection("emailSentLogs").where("productId", "==", productId));
      await deleteQueryBatch(db.collection("linkGenerationDetails").where("productId", "==", productId));

      await db.collection("products").doc(productId).delete();

      // Recompute user stats
      try {
        const [productsSnap, viewsSnap, ordersSnapAfter] = await Promise.all([
          db.collection("products").where("uid", "==", uid).get(),
          db.collection("productViews").where("sellerUid", "==", uid).get(),
          db.collection("orders").where("sellerUid", "==", uid).get(),
        ]);

        const linksCount = productsSnap.size;
        const viewsCount = viewsSnap.size;

        const orders = ordersSnapAfter.docs.map((d) => d.data());
        const ordersCount = orders.length;
        const shippedCount = orders.filter((o) => o.status === "shipped" || o.status === "delivered").length;
        const revenueCount = orders.reduce((sum, o) => {
          if (o.sellerAmount && o.status !== "cancelled") return sum + Number(o.sellerAmount || 0);
          return sum;
        }, 0);

        await db
          .collection("userStats")
          .doc(uid)
          .set(
            {
              linksCount,
              viewsCount,
              ordersCount,
              shippedCount,
              revenueCount,
              lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
            },
            { merge: true }
          );
      } catch (e) {
        console.error("Error updating userStats after delete:", e.message);
      }

      return res.status(200).json({ success: true, message: "Product and related data deleted successfully" });
    } catch (err) {
      console.error("deleteProduct error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/* ---- Orders & Links ---- */

exports.getOrders = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const snap = await db.collection("orders").where("sellerUid", "==", uid).get();
      const orders = snap.docs.map((d) => ({ id: d.id, ...d.data() }));

      const shippedCount = orders.filter((o) => o.status === "shipped" || o.status === "delivered").length;

      let totalRevenue = 0;
      orders.forEach((o) => {
        if (o.sellerAmount && o.status !== "cancelled") totalRevenue += parseFloat(o.sellerAmount);
      });

      await db
        .collection("userStats")
        .doc(uid)
        .set(
          {
            ordersCount: orders.length,
            shippedCount,
            revenueCount: totalRevenue,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );

      return res.status(200).json({
        success: true,
        orders,
        stats: { ordersCount: orders.length, shippedCount, revenueCount: totalRevenue },
      });
    } catch (err) {
      console.error("getOrders error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getLinks = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const snap = await db.collection("products").where("uid", "==", uid).get();
      const links = snap.docs.map((d) => ({
        id: d.id,
        title: d.data().title,
        price: d.data().price,
        category: d.data().category || "general",
        sales: d.data().sales || 0,
        revenue: d.data().revenue || 0,
        createdAt: d.data().createdAt,
        expiresAt: d.data().expiresAt || null,
        shareableLink: `https://www.g-z.online/product.html?productId=${d.id}`,
      }));

      await db
        .collection("userStats")
        .doc(uid)
        .set({ linksCount: links.length, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });

      const viewsSnap = await db.collection("productViews").where("sellerUid", "==", uid).get();
      const viewsByProduct = {};
      viewsSnap.docs.forEach((doc) => {
        const data = doc.data();
        if (data.productId) viewsByProduct[data.productId] = (viewsByProduct[data.productId] || 0) + 1;
      });

      links.forEach((l) => {
        l.views = viewsByProduct[l.id] || 0;
      });

      const totalViews = Object.values(viewsByProduct).reduce((s, c) => s + c, 0);

      await db
        .collection("userStats")
        .doc(uid)
        .set({ viewsCount: totalViews, lastUpdated: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });

      links.forEach((l) => {
        if (l.createdAt) l.createdAt = l.createdAt.toDate();
        if (l.expiresAt) l.expiresAt = l.expiresAt.toDate();
      });

      return res.status(200).json({ success: true, links, stats: { linksCount: links.length, viewsCount: totalViews } });
    } catch (err) {
      console.error("getLinks error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getProductDetails = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const productId = req.query.productId;
      if (!productId) return res.status(400).json({ success: false, error: "Missing productId" });

      const doc = await db.collection("products").doc(productId).get();
      if (!doc.exists) return res.status(404).json({ success: false, error: "Product not found" });

      const product = doc.data();

      // Expiré ?
      if (product.expiresAt && product.expiresAt.toDate() < new Date()) {
        return res.status(410).json({ success: false, error: "This product has expired", expired: true });
      }

      // Signed URL cover (48h)
      const coverFile = bucket.file(product.coverPath);
      const [coverUrl] = await coverFile.getSignedUrl({
        action: "read",
        expires: Date.now() + SIGNED_READ_URL_TTL_MS,
      });

      // Vue + stats
      await db.collection("productViews").add({
        productId,
        sellerUid: product.uid,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });

      const userStatsRef = db.collection("userStats").doc(product.uid);
      const userStatsDoc = await userStatsRef.get();
      if (userStatsDoc.exists) {
        await userStatsRef.update({
          viewsCount: admin.firestore.FieldValue.increment(1),
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        });
      } else {
        await userStatsRef.set({
          linksCount: 1,
          viewsCount: 1,
          ordersCount: 0,
          shippedCount: 0,
          revenueCount: 0,
          lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
        });
      }

      // Infos vendeur
      const sellerDoc = await db.collection("users").doc(product.uid).get();
      const sellerName = sellerDoc.exists ? sellerDoc.data().displayName || "Anonymous" : "Anonymous";

      // Heures restantes
      let expirationDate = null;
      let hoursRemaining = null;
      if (product.expiresAt) {
        expirationDate = product.expiresAt.toDate();
        const diffMs = expirationDate - new Date();
        hoursRemaining = Math.ceil(diffMs / (60 * 60 * 1000));
      }

      return res.status(200).json({
        success: true,
        product: {
          id: doc.id,
          title: product.title,
          description: product.description,
          price: product.price,
          category: product.category,
          coverUrl,
          sellerName,
          sellerUid: product.uid,
          expiresAt: expirationDate,
          hoursRemaining,
        },
      });
    } catch (err) {
      console.error("getProductDetails error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.collectBuyerEmail = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const { email, productId } = req.body || {};
      if (!email || !productId) return res.status(400).json({ success: false, error: "Missing email or productId" });
      if (!isEmail(email)) return res.status(400).json({ success: false, error: "Invalid email format" });

      const productDoc = await db.collection("products").doc(productId).get();
      if (!productDoc.exists) return res.status(404).json({ success: false, error: "Product not found" });

      const product = productDoc.data();
      if (product.expiresAt && product.expiresAt.toDate() < new Date()) {
        return res.status(410).json({ success: false, error: "This product has expired", expired: true });
      }

      const sessionId = generateUniqueAccessToken(16);
      await db.collection("paymentSessions").doc(sessionId).set({
        email,
        productId,
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        completed: false,
      });

      return res.status(200).json({ success: true, sessionId, message: "Email collected successfully" });
    } catch (err) {
      console.error("collectBuyerEmail error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/* ============================= STRIPE CHECKOUT ============================= */

/**
 * Créer une session Stripe Checkout pour un produit donné.
 * Requiert: productId, sessionId (issu de collectBuyerEmail).
 */
exports.createStripeCheckoutSession = onRequest({ secrets: [stripeSecretKey] }, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      if (!stripe) {
        stripe = require("stripe")(stripeSecretKey.value());
      }

      const { productId, sessionId, successUrl, cancelUrl } = req.body || {};
      if (!productId || !sessionId)
        return res.status(400).json({ success: false, error: "Missing productId or sessionId" });

      // Validate session
      const sessionDoc = await db.collection("paymentSessions").doc(sessionId).get();
      if (!sessionDoc.exists) return res.status(404).json({ success: false, error: "Session not found" });
      if (sessionDoc.data().completed) return res.status(400).json({ success: false, error: "Session already completed" });

      // Validate product
      const productDoc = await db.collection("products").doc(productId).get();
      if (!productDoc.exists) return res.status(404).json({ success: false, error: "Product not found" });

      const product = productDoc.data();
      if (product.expiresAt && product.expiresAt.toDate() < new Date()) {
        return res.status(410).json({ success: false, error: "This product has expired", expired: true });
      }

      // Optional: generate a short-lived cover URL for Stripe images
      let checkoutImageUrl = undefined;
      try {
        if (product.coverPath) {
          const [coverSigned] = await bucket.file(product.coverPath).getSignedUrl({
            action: "read",
            expires: Date.now() + 15 * 60 * 1000, // 15 minutes
          });
          checkoutImageUrl = coverSigned;
        }
      } catch (e) {
        console.error("Could not sign cover for checkout image:", e.message);
      }

      const unitAmount = Math.round(Number(product.price) * 100);

      const successReturn =
        successUrl ||
        `https://www.g-z.online/product.html?productId=${productId}&success=true&session=${encodeURIComponent(
          sessionId
        )}`;
      const cancelReturn = cancelUrl || `https://www.g-z.online/product.html?productId=${productId}&cancel=true`;

      // metadata pour retrouver plus tard
      const metadata = {
        app_session_id: sessionId,
        app_product_id: productId,
        app_seller_uid: product.uid,
        app_product_title: product.title || "",
      };

      const session = await stripe.checkout.sessions.create({
        mode: "payment",
        payment_method_types: ["card"],
        line_items: [
          {
            price_data: {
              currency: "usd",
              product_data: {
                name: product.title,
                description: product.description || undefined,
                images: checkoutImageUrl ? [checkoutImageUrl] : [],
              },
              unit_amount: unitAmount,
            },
            quantity: 1,
          },
        ],
        customer_email: sessionDoc.data().email || undefined,
        success_url: successReturn,
        cancel_url: cancelReturn,
        metadata,
      });

      // On stocke l’id stripeSessionId
      await db.collection("paymentSessions").doc(sessionId).update({
        stripeSessionId: session.id,
        stripeStatus: "created",
      });

      return res.status(200).json({
        success: true,
        sessionId: session.id,
        url: session.url,
      });
    } catch (err) {
      console.error("createStripeCheckoutSession error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/**
 * Webhook Stripe: fulfillment après paiement réussi
 * Écoute: checkout.session.completed
 * Remarque: on utilise raw body pour vérifier la signature.
 */
exports.stripeWebhook = onRequest(
  {
    secrets: [stripeSecretKey, stripeWebhookSecret],
    region: "us-central1",
    memory: "256MiB",
    cors: ["*"],
  },
  async (req, res) => {
    // Stripe exige le raw body pour la signature
    let buf;
    try {
      buf = await rawBodySaver(req);
    } catch (e) {
      console.error("Error getting raw body:", e);
      res.status(400).send("Invalid body");
      return;
    }

    const sig = req.get("Stripe-Signature");
    try {
      if (!stripe) stripe = require("stripe")(stripeSecretKey.value());
      const event = stripe.webhooks.constructEvent(buf, sig, stripeWebhookSecret.value());

      if (event.type === "checkout.session.completed") {
        const session = event.data.object;

        const appSessionId = session.metadata?.app_session_id;
        const productId = session.metadata?.app_product_id;
        const sellerUid = session.metadata?.app_seller_uid;
        const productTitle = session.metadata?.app_product_title || "Product";
        const buyerEmail = session.customer_details?.email || session.customer_email;

        if (!appSessionId || !productId || !sellerUid || !buyerEmail) {
          console.error("Missing metadata for fulfillment", session.id);
          res.json({ received: true });
          return;
        }

        // Idempotence: vérifier si déjà complété
        const paymentSessionRef = db.collection("paymentSessions").doc(appSessionId);
        const paymentSessionDoc = await paymentSessionRef.get();
        if (!paymentSessionDoc.exists) {
          console.error("Payment session not found:", appSessionId);
          res.json({ received: true });
          return;
        }
        if (paymentSessionDoc.data().completed) {
          res.json({ received: true });
          return;
        }

        // Valider produit et non expiré
        const productDoc = await db.collection("products").doc(productId).get();
        if (!productDoc.exists) {
          console.error("Product not found:", productId);
          res.json({ received: true });
          return;
        }
        const product = productDoc.data();
        if (product.expiresAt && product.expiresAt.toDate() < new Date()) {
          console.warn("Product expired at fulfillment:", productId);
          res.json({ received: true });
          return;
        }

        // Fichier présent ?
        const productFile = bucket.file(product.filePath);
        const [fileExists] = await productFile.exists();
        if (!fileExists) {
          console.error("Product file missing at fulfillment:", product.filePath);
          res.json({ received: true });
          return;
        }

        // Calculs frais identiques à PayPal (commission plateforme + approx frais Stripe)
        const productPrice = Number(product.price);
        // Stripe fee estimé (varie) — tu peux adapter à tes relevés: ~2.9% + $0.30 US
        const STRIPE_FEE_RATE = 0.029;
        const STRIPE_FEE_FIXED = 0.3;
        const stripeFee = productPrice * STRIPE_FEE_RATE + STRIPE_FEE_FIXED;
        const monetizeltCommission = productPrice * PLATFORM_RATE;
        const sellerAmount = productPrice - stripeFee - monetizeltCommission;

        const accessToken = generateUniqueAccessToken();
        // On n'a pas le userAgent ici: deviceInfo minimal
        const deviceInfo = {
          browser: "Unknown",
          os: "Unknown",
          device: "Unknown",
          userAgent: "",
        };

        const orderRef = await db.collection("orders").add({
          productId,
          productTitle,
          buyerEmail,
          sellerUid,
          stripeSessionId: session.id,
          stripePaymentIntent: session.payment_intent || null,
          amount: productPrice,
          stripeFee,
          commission: monetizeltCommission,
          sellerAmount,
          status: "completed",
          accessToken,
          accessUrl: `https://www.g-z.online/access.html?token=${accessToken}`,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          deviceInfo,
          gateway: "stripe",
        });

        await db.collection("products").doc(productId).update({
          sales: admin.firestore.FieldValue.increment(1),
          revenue: admin.firestore.FieldValue.increment(sellerAmount),
        });

        await db
          .collection("users")
          .doc(sellerUid)
          .set(
            { balance: admin.firestore.FieldValue.increment(sellerAmount), lastSale: admin.firestore.FieldValue.serverTimestamp() },
            { merge: true }
          );

        await db.collection("transactions").add({
          userId: sellerUid,
          productId,
          orderId: orderRef.id,
          type: "sale",
          amount: sellerAmount,
          grossAmount: productPrice,
          stripeFee,
          commission: monetizeltCommission,
          date: admin.firestore.FieldValue.serverTimestamp(),
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          status: "completed",
          buyerEmail,
          gateway: "stripe",
        });

        const userStatsRef = db.collection("userStats").doc(sellerUid);
        const userStatsDoc = await userStatsRef.get();
        if (userStatsDoc.exists) {
          await userStatsRef.update({
            ordersCount: admin.firestore.FieldValue.increment(1),
            revenueCount: admin.firestore.FieldValue.increment(sellerAmount),
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          });
        } else {
          await userStatsRef.set({
            linksCount: 1,
            viewsCount: 0,
            ordersCount: 1,
            shippedCount: 0,
            revenueCount: sellerAmount,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          });
        }

        await paymentSessionRef.update({
          completed: true,
          orderId: orderRef.id,
          stripeStatus: "completed",
        });

        // Email acheteur de confirmation avec lien d'accès
        try {
          const sgMail = require("@sendgrid/mail");
          sgMail.setApiKey(sendgridApiKey.value());

          const colors = {
            primary: "#007bff",
            secondary: "#20c997",
            accent: "#6f42c1",
            light: "#f8f9fa",
            dark: "#000000",
          };
          const profileImageUrl =
            "https://firebasestorage.googleapis.com/v0/b/monetizelt-b235d.appspot.com/o/brand%2Favatar.png?alt=media";

          await sgMail.send({
            to: buyerEmail,
            from: { email: "noreply@g-z.online", name: "Monetizelt" },
            subject: `Your purchase of ${productTitle} is confirmed!`,
            html: `
              <div style="font-family:Segoe UI,Tahoma,Geneva,Verdana,sans-serif;max-width:500px;margin:0 auto;padding:15px;background:${colors.dark};color:white;border-radius:6px;">
                <div style="text-align:center;margin-bottom:15px;">
                  <div style="display:inline-block;width:40px;height:40px;border-radius:50%;overflow:hidden;margin-bottom:5px;">
                    <img src="${profileImageUrl}" alt="Profile" style="width:100%;height:100%;object-fit:cover;">
                  </div>
                  <p style="margin:5px 0 0 0;font-size:12px;color:#aaa;">Digital Content Marketplace</p>
                </div>
                <div style="background:#111;padding:15px;border-radius:10px;border-top:2px solid ${colors.primary};">
                  <h2 style="margin-top:0;font-size:16px;">Purchase Confirmed</h2>
                  <p style="font-size:14px;color:#ddd;line-height:1.4;">You have successfully purchased <strong>${productTitle}</strong>.</p>
                  <div style="margin:15px 0;padding:10px;background:#222;border-radius:10px;border-left:3px solid ${colors.primary};">
                    <p style="margin:5px 0;"><strong>Product:</strong> ${productTitle}</p>
                    <p style="margin:5px 0;"><strong>Price:</strong> $${productPrice.toFixed(2)}</p>
                  </div>
                  <p style="font-size:14px;color:#ddd;line-height:1.4;">To access your content, click the button below:</p>
                  <div style="text-align:center;margin-top:15px;">
                    <a href="https://www.g-z.online/access.html?token=${accessToken}" style="background:${colors.primary};color:white;padding:8px 16px;text-decoration:none;border-radius:10px;font-weight:bold;display:inline-block;font-size:14px;">Access Content</a>
                  </div>
                  <p style="font-size:12px;color:#999;line-height:1.4;margin-top:15px;">This link is unique to you and should not be shared.</p>
                </div>
                <div style="margin-top:15px;font-size:11px;color:#777;text-align:center;">© ${new Date().getFullYear()} Monetizelt</div>
              </div>
            `,
          });
        } catch (e) {
          console.error("Error sending buyer confirmation email:", e.message);
        }

        // Email vendeur
        try {
          const sellerDoc = await db.collection("users").doc(sellerUid).get();
          const sellerEmail = sellerDoc.exists ? sellerDoc.data().email : null;
          if (sellerEmail && isEmail(sellerEmail)) {
            await sendEmailNotification("sale_notification", {
              sellerEmail,
              productTitle,
              amount: productPrice,
              sellerAmount,
            });
          }
        } catch (e) {
          console.error("Error sending seller sale_notification:", e.message);
        }
      }

      res.json({ received: true });
    } catch (err) {
      console.error("Stripe webhook error:", err.message);
      res.status(400).send(`Webhook Error: ${err.message}`);
    }
  }
);

/* ---- Access content ---- */

exports.accessContent = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const { token, userAgent } = req.query;
      if (!token) return res.status(400).json({ success: false, error: "Missing access token" });

      const ordersSnap = await db.collection("orders").where("accessToken", "==", token).limit(1).get();
      if (ordersSnap.empty)
        return res.status(404).json({
          success: false,
          error: "Invalid access token. You can only access content you have purchased.",
        });

      const order = ordersSnap.docs[0].data();
      const deviceInfo = extractDeviceInfo(userAgent);
      const originalDevice = order.deviceInfo || {};
      const isSameDevice =
        deviceInfo.browser === (originalDevice.browser || "") && deviceInfo.os === (originalDevice.os || "");

      if (!isSameDevice) {
        await db.collection("accessAttempts").add({
          orderId: ordersSnap.docs[0].id,
          originalDevice,
          attemptDevice: deviceInfo,
          timestamp: admin.firestore.FieldValue.serverTimestamp(),
          allowed: false,
        });

        return res.status(403).json({
          success: false,
          error: "You can only access this content from the device used for purchase.",
        });
      }

      const productDoc = await db.collection("products").doc(order.productId).get();
      if (!productDoc.exists) return res.status(404).json({ success: false, error: "Product not found" });
      const product = productDoc.data();

      // Signed read URLs (48h)
      const productFile = bucket.file(product.filePath);
      const [fileUrl] = await productFile.getSignedUrl({
        action: "read",
        expires: Date.now() + SIGNED_READ_URL_TTL_MS,
      });

      const coverFile = bucket.file(product.coverPath);
      const [coverUrl] = await coverFile.getSignedUrl({
        action: "read",
        expires: Date.now() + SIGNED_READ_URL_TTL_MS,
      });

      await db.collection("accessLogs").add({
        orderId: ordersSnap.docs[0].id,
        productId: order.productId,
        buyerEmail: order.buyerEmail,
        deviceInfo,
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });

      if (order.status === "completed") {
        await db.collection("orders").doc(ordersSnap.docs[0].id).update({
          status: "shipped",
          deliveredAt: admin.firestore.FieldValue.serverTimestamp(),
        });

        await db
          .collection("userStats")
          .doc(product.uid)
          .set(
            { shippedCount: admin.firestore.FieldValue.increment(1), lastUpdated: admin.firestore.FieldValue.serverTimestamp() },
            { merge: true }
          );
      }

      const fileName = product.filePath.split("/").pop();
      const ext = (fileName.split(".").pop() || "").toLowerCase();
      let contentType = "other";
      if (["mp3", "wav", "ogg"].includes(ext)) contentType = "audio";
      else if (["mp4", "webm", "mov"].includes(ext)) contentType = "video";
      else if (["pdf", "epub"].includes(ext)) contentType = "document";

      return res.status(200).json({
        success: true,
        product: {
          title: product.title,
          description: product.description,
          contentType,
          fileUrl,
          coverUrl,
          fileExtension: ext,
          category: product.category,
        },
      });
    } catch (err) {
      console.error("accessContent error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

exports.getSellerStats = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  return corsMiddleware(req, res, async () => {
    try {
      const uid = req.query.uid;
      if (!uid) return res.status(400).json({ success: false, error: "Missing uid" });

      const productsSnap = await db.collection("products").where("uid", "==", uid).get();
      const productIds = productsSnap.docs.map((d) => d.id);

      let orders = [];
      if (productIds.length > 0) {
        for (let i = 0; i < productIds.length; i += 10) {
          const chunk = productIds.slice(i, i + 10);
          const ordersSnap = await db.collection("orders").where("productId", "in", chunk).get();
          orders = orders.concat(ordersSnap.docs.map((d) => d.data()));
        }
      }

      const totalSales = orders.length;
      const totalRevenue = orders.reduce((sum, o) => sum + (o.amount || 0), 0);
      const totalPaypalFees = orders.reduce((sum, o) => sum + (o.paypalFee || 0), 0); // legacy champs possibles
      const totalStripeFees = orders.reduce((sum, o) => sum + (o.stripeFee || 0), 0);
      const totalCommission = orders.reduce((sum, o) => sum + (o.commission || 0), 0);
      const netIncome = orders.reduce((sum, o) => sum + (o.sellerAmount || 0), 0);
      const shippedCount = orders.filter((o) => o.status === "shipped" || o.status === "delivered").length;

      const productStats = {};
      productsSnap.docs.forEach((doc) => {
        const p = doc.data();
        productStats[doc.id] = {
          title: p.title,
          sales: p.sales || 0,
          revenue: p.revenue || 0,
          expiresAt: p.expiresAt ? p.expiresAt.toDate() : null,
        };
      });

      const viewsSnap = await db.collection("productViews").where("sellerUid", "==", uid).get();
      const totalViews = viewsSnap.size;

      await db
        .collection("userStats")
        .doc(uid)
        .set(
          {
            linksCount: productIds.length,
            viewsCount: totalViews,
            ordersCount: totalSales,
            shippedCount,
            revenueCount: netIncome,
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );

      return res.status(200).json({
        success: true,
        stats: {
          totalSales,
          totalRevenue,
          totalPaypalFees,
          totalStripeFees,
          totalCommission,
          netIncome,
          productCount: productIds.length,
          products: productStats,
          viewsCount: totalViews,
          shippedCount,
        },
      });
    } catch (err) {
      console.error("getSellerStats error:", err);
      return res.status(500).json({ success: false, error: err.message || "Internal error" });
    }
  });
});

/* ---- Weekly payouts via Pub/Sub (optionnel) ---- */

exports.weeklyPayouts = onMessagePublished(
  {
    topic: "weekly-payouts-topic",
    secrets: [paypalClientId, paypalClientSecret, sendgridApiKey],
    region: "us-central1",
  },
  async () => {
    console.log("🚀 Weekly payouts start - Friday batch processing");

    try {
      const allUsersSnap = await db.collection("users").where("balance", ">", 0).get();

      const eligibleUsers = [];
      const ineligibleUsers = [];

      allUsersSnap.docs.forEach((doc) => {
        const u = { id: doc.id, ...doc.data(), balance: Number(doc.data().balance || 0) };
        if (u.balance >= MIN_PAYOUT && u.paypalEmail && isEmail(u.paypalEmail)) eligibleUsers.push(u);
        else if (u.balance > 0 && u.paypalEmail && isEmail(u.paypalEmail)) ineligibleUsers.push(u);
      });

      for (const user of ineligibleUsers) {
        try {
          await sendEmailNotification("min_balance_not_reached", { email: user.paypalEmail, balance: user.balance });
        } catch (err) {
          console.error(`Error sending low-balance email to ${user.id}:`, err.message || err);
        }
      }

      eligibleUsers.sort((a, b) => b.balance - a.balance);

      const processOne = async (user) => {
        try {
          let firstName = "Seller";
          try {
            const userDoc = await db.collection("users").doc(user.id).get();
            if (userDoc.exists) {
              const ud = userDoc.data();
              firstName = (ud.displayName || ud.firstName || "").split(" ")[0] || "Seller";
            }
          } catch (e) {
            console.error(`Error getting user name for ${user.id}:`, e);
          }

          await processPayout(user.id, user.balance, user.paypalEmail);
          const payoutFee = PAYPAL_FEE_FIXED + user.balance * PAYPAL_FEE_RATE;
          const netAmount = user.balance - payoutFee;

          await sendEmailNotification("payout_notification", {
            paypalEmail: user.paypalEmail,
            amount: netAmount,
            firstName,
            userId: user.id,
          });
        } catch (err) {
          console.error(`Payout failed for ${user.id}:`, err.message || err);
          await db.collection("payoutErrors").add({
            userId: user.id,
            amount: user.balance,
            paypalEmail: user.paypalEmail,
            error: err.message || "Unknown error",
            timestamp: admin.firestore.FieldValue.serverTimestamp(),
          });
        }
        await new Promise((r) => setTimeout(r, 100));
      };

      if (eligibleUsers.length <= 500) {
        for (const user of eligibleUsers) await processOne(user);
      } else {
        const totalBatches = Math.ceil(eligibleUsers.length / 500);
        for (let b = 0; b < totalBatches; b++) {
          const batchUsers = eligibleUsers.slice(b * 500, Math.min((b + 1) * 500, eligibleUsers.length));
          for (const user of batchUsers) await processOne(user);
          if (b < totalBatches - 1) await new Promise((r) => setTimeout(r, 5000));
        }
      }

      console.log("✅ Weekly payouts completed");
      return null;
    } catch (error) {
      console.error("❌ Error processing weekly payouts:", error);
      await db.collection("systemErrors").add({
        function: "weeklyPayouts",
        error: error.message || "Unknown error",
        timestamp: admin.firestore.FieldValue.serverTimestamp(),
      });
      return null;
    }
  }
);

/* ---- Période stats ---- */

exports.getPeriodData = onRequest({}, async (req, res) => {
  // Preflight
  res.set("Access-Control-Allow-Origin", "*");
  res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  try {
    corsMiddleware(req, res, async () => {
      const authHeader = req.headers.authorization;
      if (!authHeader || !authHeader.startsWith("Bearer "))
        return res.status(401).json({ error: "Unauthorized" });

      const idToken = authHeader.split("Bearer ")[1];
      let decodedToken;
      try {
        decodedToken = await admin.auth().verifyIdToken(idToken);
      } catch (e) {
        console.error("Error verifying token:", e);
        return res.status(401).json({ error: "Invalid token" });
      }

      const userId = decodedToken.uid;
      const { year, month } = req.query;
      if (!year || !month) return res.status(400).json({ error: "Year and month are required" });

      const startDate = new Date(parseInt(year), parseInt(month) - 1, 1);
      const endDate = new Date(parseInt(year), parseInt(month), 0, 23, 59, 59);
      const startTimestamp = admin.firestore.Timestamp.fromDate(startDate);
      const endTimestamp = admin.firestore.Timestamp.fromDate(endDate);

      try {
        const productsQuery = await db
          .collection("products")
          .where("uid", "==", userId)
          .where("createdAt", ">=", startTimestamp)
          .where("createdAt", "<=", endTimestamp)
          .get();

        const linksCount = productsQuery.size;

        const allProductsQuery = await db.collection("products").where("uid", "==", userId).get();
        const productIds = allProductsQuery.docs.map((d) => d.id);

        let viewsCount = 0;
        if (productIds.length > 0) {
          const batches = [];
          for (let i = 0; i < productIds.length; i += 10) batches.push(productIds.slice(i, i + 10));
          const viewPromises = batches.map(async (batch) => {
            const snap = await db
              .collection("productViews")
              .where("productId", "in", batch)
              .where("timestamp", ">=", startTimestamp)
              .where("timestamp", "<=", endTimestamp)
              .get();
            return snap.size;
          });
          const counts = await Promise.all(viewPromises);
          viewsCount = counts.reduce((s, c) => s + c, 0);
        }

        const ordersQuery = await db
          .collection("orders")
          .where("sellerUid", "==", userId)
          .where("createdAt", ">=", startTimestamp)
          .where("createdAt", "<=", endTimestamp)
          .get();

        let ordersCount = 0;
        let shippedCount = 0;
        let totalRevenue = 0;

        ordersQuery.forEach((doc) => {
          const o = doc.data();
          ordersCount++;
          if (o.status === "shipped" || o.status === "delivered") shippedCount++;
          if (o.sellerAmount && o.status !== "cancelled") totalRevenue += parseFloat(o.sellerAmount);
        });

        const transactionsQuery = await db
          .collection("transactions")
          .where("userId", "==", userId)
          .where("date", ">=", startTimestamp)
          .where("date", "<=", endTimestamp)
          .orderBy("date", "desc")
          .get();

        const transactions = transactionsQuery.docs.map((d) => ({
          id: d.id,
          ...d.data(),
          date: d.data().date ? d.data().date.toDate().toISOString() : null,
          createdAt: d.data().createdAt ? d.data().createdAt.toDate().toISOString() : null,
        }));

        await db.collection("userStats").doc(userId).set(
          {
            lastUpdated: admin.firestore.FieldValue.serverTimestamp(),
          },
          { merge: true }
        );

        return res.status(200).json({
          success: true,
          data: {
            period: { year: parseInt(year), month: parseInt(month) },
            stats: { linksCount, viewsCount, ordersCount, shippedCount, revenueCount: totalRevenue },
            transactions,
          },
        });
      } catch (error) {
        console.error("Error retrieving period data:", error);
        return res.status(500).json({ error: "Failed to retrieve period data", details: error.message });
      }
    });
  } catch (error) {
    console.error("Unhandled error in getPeriodData:", error);
    return res.status(500).json({ error: "Internal server error" });
  }
});
