// /api/index.js (Final and Secure Version with Limit-Based Reset)

/**
 * SHIB Ads WebApp Backend API
 * Handles all POST requests from the Telegram Mini App frontend.
 * Uses the Supabase REST API for persistence.
 */
const crypto = require('crypto');

// Load environment variables for Supabase connection
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;
// ⚠️ BOT_TOKEN must be set in Vercel environment variables
const BOT_TOKEN = process.env.BOT_TOKEN;

// ------------------------------------------------------------------
// Fully secured and defined server-side constants
// ------------------------------------------------------------------
const REWARD_PER_AD = 3;
const REFERRAL_COMMISSION_RATE = 0.05;
const DAILY_MAX_ADS = 100; // Max ads limit
const DAILY_MAX_SPINS = 15; // Max spins limit
const RESET_INTERVAL_MS = 6 * 60 * 60 * 1000; // ⬅️ 6 hours in milliseconds
const MIN_TIME_BETWEEN_ACTIONS_MS = 3000; // 3 seconds minimum time between watchAd/spin requests
const ACTION_ID_EXPIRY_MS = 60000; // 60 seconds for Action ID to be valid
const SPIN_SECTORS = [5, 10, 15, 20, 5];

// ------------------------------------------------------------------
// NEW Task/Mission Constants (Refactored to support list of missions)
// ------------------------------------------------------------------
const TELEGRAM_CHANNEL_USERNAME = '@botbababab'; // The original channel username
// New structure that defines all missions/links and their rewards
const MISSIONS_LIST = [
    {
        id: 1,
        name_ar: 'انضمام لقناة التلجرام الرسمية', // Official Telegram Channel Join
        name_en: 'Join Official Telegram Channel',
        type: 'channel_join', // Special type for DB check (uses 'task_completed' field)
        link: `https://t.me/${TELEGRAM_CHANNEL_USERNAME.substring(1)}`,
        reward: 50,
    },
    {
        id: 2,
        name_ar: 'الاشتراك في رابط الإعلانات الهام', // Subscribe to the Important Ads Link
        name_en: 'Subscribe to Important Ads Link',
        type: 'external_link', // External link type (reward upon action)
        link: 'https://external.important.link/shib', // Hypothetical important link
        reward: 75,
    },
    {
        id: 3,
        name_ar: 'متابعة قناة المكافآت اليومية', // Follow Daily Rewards Channel
        name_en: 'Follow Daily Rewards Channel',
        type: 'external_link',
        link: 'https://t.me/shib_daily_rewards', 
        reward: 100,
    },
];


/**
 * Helper function to randomly select a prize from the defined sectors and return its index.
 */
function calculateRandomSpinPrize() {
    const randomIndex = Math.floor(Math.random() * SPIN_SECTORS.length);
    const prize = SPIN_SECTORS[randomIndex];
    return { prize, prizeIndex: randomIndex };
}

// --- Helper Functions ---

function sendSuccess(res, data = {}) {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: true, data }));
}

function sendError(res, message, statusCode = 400) {
  res.writeHead(statusCode, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ ok: false, error: message }));
}

async function supabaseFetch(tableName, method, body = null, queryParams = '?select=*') {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error('Supabase environment variables are not configured.');
  }

  const url = `${SUPABASE_URL}/rest/v1/${tableName}${queryParams}`;

  const headers = {
    'apikey': SUPABASE_ANON_KEY,
    'Authorization': `Bearer ${SUPABASE_ANON_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };

  const options = {
    method,
    headers,
    body: body ? JSON.stringify(body) : null,
  };

  const response = await fetch(url, options);

  if (response.ok) {
      const responseText = await response.text();
      try {
          const jsonResponse = JSON.parse(responseText);
          return Array.isArray(jsonResponse) ? jsonResponse : { success: true };
      } catch (e) {
          return { success: true };
      }
  }

  let data;
  try {
      data = await response.json();
  } catch (e) {
      const errorMsg = `Supabase error: ${response.status} ${response.statusText}`;
      throw new Error(errorMsg);
  }

  const errorMsg = data.message || `Supabase error: ${response.status} ${response.statusText}`;
  throw new Error(errorMsg);
}

/**
 * Checks if a user is a member (or creator/admin) of a specific Telegram channel.
 */
async function checkChannelMembership(userId, channelUsername) {
    if (!BOT_TOKEN) {
        console.error('BOT_TOKEN is not configured for membership check.');
        return false;
    }
    
    // The chat_id must be in the format @username or -100xxxxxxxxxx
    const chatId = channelUsername.startsWith('@') ? channelUsername : `@${channelUsername}`; 

    const url = `https://api.telegram.org/bot${BOT_TOKEN}/getChatMember?chat_id=${chatId}&user_id=${userId}`;
    
    try {
        const response = await fetch(url);
        if (!response.ok) {
            const errorData = await response.json();
            console.error('Telegram API error (getChatMember):', errorData.description || response.statusText);
            return false;
        }

        const data = await response.json();
        
        if (!data.ok) {
             console.error('Telegram API error (getChatMember - not ok):', data.description);
             return false;
        }

        const status = data.result.status;
        
        // Accepted statuses are 'member', 'administrator', 'creator'
        const isMember = ['member', 'administrator', 'creator'].includes(status);
        
        return isMember;

    } catch (error) {
        console.error('Network or parsing error during Telegram API call:', error.message);
        return false;
    }
}


/**
 * Limit-Based Reset Logic: Resets counters if the limit was reached AND the interval (6 hours) has passed since.
 * ⚠️ هذا هو التعديل الرئيسي: يعتمد على أعمدة الوصول للحد الأقصى وليس على آخر نشاط عام.
 */
async function resetDailyLimitsIfExpired(userId) {
    const now = Date.now();

    try {
        // 1. Fetch current limits and the time they were reached
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=ads_watched_today,spins_today,ads_limit_reached_at,spins_limit_reached_at`);
        if (!Array.isArray(users) || users.length === 0) {
            return;
        }

        const user = users[0];
        const updatePayload = {};

        // 2. Check Ads Limit Reset
        if (user.ads_limit_reached_at && user.ads_watched_today >= DAILY_MAX_ADS) {
            const adsLimitTime = new Date(user.ads_limit_reached_at).getTime();
            if (now - adsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ تم مرور 6 ساعات على الوصول للحد الأقصى، يتم إعادة التعيين
                updatePayload.ads_watched_today = 0;
                updatePayload.ads_limit_reached_at = null; // إزالة الوقت لانتهاء فترة القفل
                console.log(`Ads limit reset for user ${userId}.`);
            }
        }

        // 3. Check Spins Limit Reset
        if (user.spins_limit_reached_at && user.spins_today >= DAILY_MAX_SPINS) {
            const spinsLimitTime = new Date(user.spins_limit_reached_at).getTime();
            if (now - spinsLimitTime > RESET_INTERVAL_MS) {
                // ⚠️ تم مرور 6 ساعات على الوصول للحد الأقصى، يتم إعادة التعيين
                updatePayload.spins_today = 0;
                updatePayload.spins_limit_reached_at = null; // إزالة الوقت لانتهاء فترة القفل
                console.log(`Spins limit reset for user ${userId}.`);
            }
        }

        // 4. Perform the database update if any limits were reset
        if (Object.keys(updatePayload).length > 0) {
            await supabaseFetch('users', 'PATCH',
                updatePayload,
                `?id=eq.${userId}`);
        }
    } catch (error) {
        console.error(`Failed to check/reset daily limits for user ${userId}:`, error.message);
    }
}

/**
 * Rate Limiting Check for Ad/Spin Actions
 * ⚠️ تم تعديلها: لم تعد تحدث last_activity، بل فقط تفحص الفارق الزمني الأخير
 */
async function checkRateLimit(userId) {
    try {
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${userId}&select=last_activity`);
        if (!Array.isArray(users) || users.length === 0) {
            return { ok: true };
        }

        const user = users[0];
        // إذا كان last_activity غير موجود، يمكن اعتباره 0 لضمان السماح بالمرور
        const lastActivity = user.last_activity ? new Date(user.last_activity).getTime() : 0; 
        const now = Date.now();
        const timeElapsed = now - lastActivity;

        if (timeElapsed < MIN_TIME_BETWEEN_ACTIONS_MS) {
            const remainingTime = MIN_TIME_BETWEEN_ACTIONS_MS - timeElapsed;
            return {
                ok: false,
                message: `Rate limit exceeded. Please wait ${Math.ceil(remainingTime / 1000)} seconds before the next action.`,
                remainingTime: remainingTime
            };
        }
        // تحديث last_activity سيتم لاحقاً في دوال watchAd/spinResult/completeTask
        return { ok: true };
    } catch (error) {
        console.error(`Rate limit check failed for user...`, error.message);
        return { ok: false, message: 'Internal server error during rate limit check.' };
    }
}

/**
 * Generates and saves a unique Action ID for security.
 */
async function generateActionId(res, userId, actionType) {
    const actionId = crypto.randomBytes(16).toString('hex');
    const id = parseInt(userId);

    try {
        await supabaseFetch('temp_actions', 'POST', {
            user_id: id,
            action_id: actionId,
            action_type: actionType,
            created_at: new Date().toISOString()
        });
        return actionId;
    } catch (error) {
        console.error('Failed to generate and save action ID:', error.message);
        sendError(res, 'Failed to generate security token.', 500);
    }
}

/**
 * Middleware: Checks if the Action ID is valid and then deletes it.
 */
async function validateAndUseActionId(res, userId, actionId, actionType) {
    if (!actionId) {
        sendError(res, 'Missing Server Token (Action ID). Request rejected.', 400);
        return false;
    }
    const id = parseInt(userId);

    try {
        const query = `?user_id=eq.${id}&action_id=eq.${actionId}&action_type=eq.${actionType}&select=id,created_at`;
        const records = await supabaseFetch('temp_actions', 'GET', null, query);

        if (!Array.isArray(records) || records.length === 0) {
            sendError(res, 'Invalid or previously used Server Token (Action ID).', 409);
            return false;
        }

        const record = records[0];
        const recordTime = new Date(record.created_at).getTime();
        const now = Date.now();

        // 1. Check expiry
        if (now - recordTime > ACTION_ID_EXPIRY_MS) {
            sendError(res, 'Expired Server Token (Action ID). Please try action again.', 408);
            return false;
        }

        // 2. Delete the used action ID (prevent replay attack)
        await supabaseFetch('temp_actions', 'DELETE', null, `?id=eq.${record.id}`);

        return true;

    } catch (error) {
        console.error('Action ID validation failed:', error.message);
        sendError(res, 'Security check failed due to server error.', 500);
        return false;
    }
}

/**
 * Handles referral commission logic (called silently).
 */
async function processCommission(referrerId, refereeId, sourceReward) {
    // 1. Calculate commission
    const commissionAmount = sourceReward * REFERRAL_COMMISSION_RATE;
    if (commissionAmount < 0.000001) { 
        console.log(`Commission too small (${commissionAmount}). Aborted for referee ${refereeId}.`);
        return { ok: false, error: 'Commission amount is effectively zero.' };
    }

    try {
        // 2. Fetch referrer's current balance and status
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${referrerId}&select=balance,is_banned`);
        if (!Array.isArray(users) || users.length === 0 || users[0].is_banned) {
            console.log(`Referrer ${referrerId} not found or banned. Commission aborted.`);
            return { ok: false, error: 'Referrer not found or banned, commission aborted.' };
        }
        
        // 3. Update balance: newBalance will now include the decimal commission
        const newBalance = users[0].balance + commissionAmount;

        // 4. Update referrer balance
        await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${referrerId}`);

        // 5. Log the commission (assuming a 'commissions' table)
        await supabaseFetch('commissions', 'POST', {
            referrer_id: referrerId,
            referee_id: refereeId,
            amount: commissionAmount,
            source_reward: sourceReward,
            created_at: new Date().toISOString()
        });

        return { ok: true, amount: commissionAmount };

    } catch (error) {
        console.error(`Error processing commission for referrer ${referrerId}:`, error.message);
        return { ok: false, error: error.message };
    }
}

/**
 * Validates the Telegram initData hash.
 */
function validateInitData(initData) {
    // ... (existing validateInitData logic remains the same)
    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    params.delete('hash');
    params.sort();

    let dataCheckString = '';
    for (const [key, value] of params.entries()) {
        dataCheckString += `${key}=${value}\n`;
    }
    dataCheckString = dataCheckString.trim();

    const secretKey = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
    const calculatedHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

    return calculatedHash === hash;
}


/**
 * 2) type: "getUserData"
 * ⚠️ Modification: Now returns the missions list with completion status.
 */
async function handleGetUserData(req, res, body) {
  const { user_id } = body;
  const id = parseInt(user_id);

  try {
    // 0. Perform daily limit reset checks
    await resetDailyLimitsIfExpired(id);

    // 1. Fetch user data, including the old 'task_completed' and the new 'missions_completed' array
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=*,missions_completed`); // missions_completed is assumed to be a JSONB/ARRAY column
    if (!Array.isArray(users) || users.length === 0) {
      // User doesn't exist, prompt for registration
      return sendSuccess(res, { needs_registration: true });
    }

    const userData = users[0];

    // 2. Count referrals
    const referrals = await supabaseFetch('users', 'GET', null, `?ref_by=eq.${id}&select=id`);
    const referralsCount = Array.isArray(referrals) ? referrals.length : 0;

    // 3. Fetch withdrawal history (assuming it's a separate table 'withdrawals')
    const withdrawalHistory = await supabaseFetch('withdrawals', 'GET', null, `?user_id=eq.${id}&select=amount,status,created_at`);
    
    // 4. Construct missions status list
    const completedMissionsIds = userData.missions_completed || [];

    const missionsStatus = MISSIONS_LIST.map(mission => ({
        id: mission.id,
        name_ar: mission.name_ar,
        link: mission.link,
        reward: mission.reward,
        type: mission.type,
        // Check completion: use 'task_completed' for ID 1, use 'missions_completed' array for others
        is_completed: mission.id === 1 
            ? userData.task_completed // Old field for compatibility
            : completedMissionsIds.includes(mission.id),
    }));

    // 5. Update last_activity and send response
    // Only update last_activity if not banned and data fetched successfully
    if (!userData.is_banned) { 
        await supabaseFetch('users', 'PATCH', { last_activity: new Date().toISOString() }, `?id=eq.${id}&select=id`);
    }

    sendSuccess(res, { 
        ...userData, 
        referrals_count: referralsCount, 
        withdrawal_history: withdrawalHistory,
        missions_status: missionsStatus, // NEW FIELD
    });

  } catch (error) {
    console.error('GetUserData failed:', error.message);
    sendError(res, `Failed to retrieve user data: ${error.message}`, 500);
  }
}

/**
 * 1) type: "register"
 * ⚠️ Fix: Includes task_completed: false and missions_completed: [] for new users.
 */
async function handleRegister(req, res, body) {
  const { user_id, ref_by } = body;
  const id = parseInt(user_id);

  try {
    // 1. Check if user exists
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=id,is_banned`);
    if (Array.isArray(users) && users.length > 0) {
      if (users[0].is_banned) {
         return sendError(res, 'User is banned.', 403);
      }
      return sendError(res, 'User already registered.', 409);
    }

    // 2. User doesn't exist, proceed with registration
    const insertPayload = {
      id: id,
      balance: 0,
      ads_watched_today: 0,
      spins_today: 0,
      ref_by: ref_by || null,
      task_completed: false, // Initial value
      missions_completed: [], // NEW FIELD: Empty array for new missions
      created_at: new Date().toISOString(),
      last_activity: new Date().toISOString(),
      ads_limit_reached_at: null,
      spins_limit_reached_at: null,
      is_banned: false,
    };

    await supabaseFetch('users', 'POST', insertPayload);

    // 3. Success
    sendSuccess(res, { message: 'Registration successful.' });

  } catch (error) {
    console.error('Register failed:', error.message);
    sendError(res, `Failed to register user: ${error.message}`, 500);
  }
}

/**
 * 3) type: "watchAd"
 */
async function handleWatchAd(req, res, body) {
  const { user_id, action_id } = body;
  const id = parseInt(user_id);

  // 1. Check & Use Action ID (Security)
  if (!await validateAndUseActionId(res, id, action_id, 'watchAd')) {
    return;
  }

  try {
    // 2. Fetch User Data
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,ads_watched_today,is_banned`);
    if (!Array.isArray(users) || users.length === 0) {
      return sendError(res, 'User not found.', 404);
    }
    const user = users[0];

    // 3. Banned Check
    if (user.is_banned) {
      return sendError(res, 'User is banned.', 403);
    }

    // 4. Rate Limit Check
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) {
        return sendError(res, rateLimitResult.message, 429);
    }

    // 5. Check maximum ad limit
    if (user.ads_watched_today >= DAILY_MAX_ADS) {
      return sendError(res, `Daily ad limit (${DAILY_MAX_ADS}) reached.`, 403);
    }

    // 6. Process Reward and Update User Data
    const reward = REWARD_PER_AD;
    const newBalance = user.balance + reward;
    const newAdsCount = user.ads_watched_today + 1;

    const updatePayload = {
        balance: newBalance,
        ads_watched_today: newAdsCount,
        last_activity: new Date().toISOString() // ⬅️ تحديث لـ Rate Limit
    };

    // 7. ⚠️ NEW LOGIC: Check if the limit is reached NOW
    if (newAdsCount >= DAILY_MAX_ADS) {
        updatePayload.ads_limit_reached_at = new Date().toISOString();
    }

    // 8. Update user record
    await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

    // 9. Commission Call
    if (user.ref_by) {
      processCommission(user.ref_by, id, reward).catch(e => {
        console.error(`WatchAd Commission failed silently for referrer ${user.ref_by}:`, e.message);
      });
    }

    // 10. Success
    sendSuccess(res, {
      new_balance: newBalance,
      actual_reward: reward,
      new_ads_count: newAdsCount
    });

  } catch (error) {
    console.error('WatchAd failed:', error.message);
    sendError(res, `Failed to process ad reward: ${error.message}`, 500);
  }
}

/**
 * 4) type: "commission" (Called from WatchAd/SpinResult)
 */
async function handleCommission(req, res, body) {
    const { referrer_id, referee_id, source_reward } = body;
    const result = await processCommission(referrer_id, referee_id, source_reward);

    if (result.ok) {
        sendSuccess(res, { message: 'Commission processed.', amount: result.amount });
    } else {
        // Send a non-critical success for client, but log error
        sendSuccess(res, { message: 'Commission skipped.', error: result.error });
    }
}

/**
 * 5) type: "preSpin" (Security check before wheel spin)
 */
async function handlePreSpin(req, res, body) {
    const { user_id, action_id } = body;
    const id = parseInt(user_id);
    
    // 1. Check & Use Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'preSpin')) { 
        return;
    }

    try {
        // 2. Fetch User Data
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=spins_today,is_banned`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        // 3. Banned Check
        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }
        
        // 4. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 5. Check maximum spin limit
        if (user.spins_today >= DAILY_MAX_SPINS) {
            return sendError(res, `Daily spin limit (${DAILY_MAX_SPINS}) reached.`, 403);
        }
        
        // 6. Success
        sendSuccess(res, { message: 'Pre-spin successful. Ready for ad.' });

    } catch (error) {
        console.error('PreSpin failed:', error.message);
        sendError(res, `Failed to validate pre-spin: ${error.message}`, 500);
    }
}

/**
 * 6) type: "spinResult"
 */
async function handleSpinResult(req, res, body) {
  const { user_id, action_id } = body;
  const id = parseInt(user_id);

  // 1. Check & Use Action ID (Security)
  if (!await validateAndUseActionId(res, id, action_id, 'spinResult')) {
    return;
  }

  try {
    // 2. Fetch User Data
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,spins_today,ref_by,is_banned`);
    if (!Array.isArray(users) || users.length === 0) {
      return sendError(res, 'User not found.', 404);
    }
    const user = users[0];

    // 3. Banned Check
    if (user.is_banned) {
      return sendError(res, 'User is banned.', 403);
    }

    // 4. Rate Limit Check
    const rateLimitResult = await checkRateLimit(id);
    if (!rateLimitResult.ok) {
        return sendError(res, rateLimitResult.message, 429);
    }

    // 5. Check maximum spin limit
    if (user.spins_today >= DAILY_MAX_SPINS) {
      return sendError(res, `Daily spin limit (${DAILY_MAX_SPINS}) reached.`, 403);
    }

    // --- All checks passed: Process Spin Result ---
    const { prize, prizeIndex } = calculateRandomSpinPrize();
    const newSpinsCount = user.spins_today + 1;
    const newBalance = user.balance + prize;

    const updatePayload = {
        balance: newBalance,
        spins_today: newSpinsCount,
        last_activity: new Date().toISOString() // ⬅️ تحديث لـ Rate Limit
    };
    
    // 6. ⚠️ NEW LOGIC: Check if the limit is reached NOW
    if (newSpinsCount >= DAILY_MAX_SPINS) {
        updatePayload.spins_limit_reached_at = new Date().toISOString();
    }

    // 7. Update user record
    await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

    // 8. Commission Call
    if (user.ref_by) {
        processCommission(user.ref_by, id, prize).catch(e => {
            console.error(`SpinResult Commission failed silently for referrer ${user.ref_by}:`, e.message);
        });
    }

    // 9. Success
    sendSuccess(res, { 
        prize: prize, 
        prize_index: prizeIndex, 
        new_balance: newBalance,
        new_spins_count: newSpinsCount 
    });

  } catch (error) {
    console.error('SpinResult failed:', error.message);
    sendError(res, `Failed to process spin reward: ${error.message}`, 500);
  }
}

/**
 * 7) type: "completeTask" (now handles all missions)
 * ⚠️ Major Modification: Now accepts mission_id and handles different mission types.
 */
async function handleCompleteTask(req, res, body) {
    const { user_id, action_id, mission_id } = body;
    const id = parseInt(user_id);
    
    // Default to the original task ID (1) if mission_id is missing, for compatibility
    const targetMissionId = parseInt(mission_id) || 1; 

    const mission = MISSIONS_LIST.find(m => m.id === targetMissionId);

    if (!mission) {
        return sendError(res, 'Invalid mission ID.', 404);
    }

    const reward = mission.reward;

    // 1. Check & Use Action ID (Security)
    if (!await validateAndUseActionId(res, id, action_id, 'completeTask')) { 
        return;
    }

    try {
        // 2. Fetch User Data & check completion status
        // Select 'missions_completed' which is the new array/JSONB column
        const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,is_banned,task_completed,missions_completed`);
        if (!Array.isArray(users) || users.length === 0) {
            return sendError(res, 'User not found.', 404);
        }
        const user = users[0];

        if (user.is_banned) {
            return sendError(res, 'User is banned.', 403);
        }

        // Check if this specific mission is already completed
        const completedMissionsIds = user.missions_completed || [];
        const isAlreadyCompleted = targetMissionId === 1 
            ? user.task_completed 
            : completedMissionsIds.includes(targetMissionId);

        if (isAlreadyCompleted) {
            return sendError(res, 'Mission already completed. Reward already claimed.', 409);
        }

        // 3. Rate Limit Check
        const rateLimitResult = await checkRateLimit(id);
        if (!rateLimitResult.ok) {
            return sendError(res, rateLimitResult.message, 429);
        }

        // 4. Mission-specific validation (ONLY for channel_join)
        if (mission.type === 'channel_join') {
            const isMember = await checkChannelMembership(id, TELEGRAM_CHANNEL_USERNAME);

            if (!isMember) {
                // Return a specific error message for front-end to handle
                return sendError(res, 'User is not a member of the required Telegram channel.', 400);
            }
        }
        
        // 5. Process Reward and Update User Data
        const newBalance = user.balance + reward;
        const updatePayload = {
            balance: newBalance,
            last_activity: new Date().toISOString() // Update for Rate Limit
        };

        if (targetMissionId === 1) {
            updatePayload.task_completed = true; // Mark old field as completed
        } else {
            // For new missions, update the missions_completed array (assumed JSONB/ARRAY type)
            const updatedMissionsCompleted = [...completedMissionsIds, targetMissionId];
            updatePayload.missions_completed = updatedMissionsCompleted; 
        }
        
        await supabaseFetch('users', 'PATCH', updatePayload, `?id=eq.${id}`);

        // 6. Success
        sendSuccess(res, {
            new_balance: newBalance,
            actual_reward: reward,
            message: `Mission ID ${targetMissionId} completed successfully.`,
            completed_mission_id: targetMissionId // Return ID for front-end update
        });

    } catch (error) {
        console.error('CompleteTask/Mission failed:', error.message);
        sendError(res, `Failed to complete mission: ${error.message}`, 500);
    }
}


/**
 * 8) type: "withdraw"
 */
async function handleWithdraw(req, res, body) {
  const { user_id, binanceId, amount, action_id } = body;
  const id = parseInt(user_id);
  const withdrawalAmount = parseFloat(amount);

  // 1. Check & Use Action ID (Security)
  if (!await validateAndUseActionId(res, id, action_id, 'withdraw')) {
    return;
  }

  try {
    // 2. Fetch User Data
    const users = await supabaseFetch('users', 'GET', null, `?id=eq.${id}&select=balance,is_banned`);
    if (!Array.isArray(users) || users.length === 0) {
      return sendError(res, 'User not found.', 404);
    }
    const user = users[0];

    // 3. Banned Check
    if (user.is_banned) {
      return sendError(res, 'User is banned.', 403);
    }
    
    // 4. Validate Amount and Balance
    if (isNaN(withdrawalAmount) || withdrawalAmount <= 0) {
      return sendError(res, 'Invalid withdrawal amount.', 400);
    }
    if (withdrawalAmount > user.balance) {
      return sendError(res, 'Insufficient balance.', 400);
    }
    if (withdrawalAmount < MIN_WITHDRAWAL) {
      return sendError(res, `Minimum withdrawal is ${MIN_WITHDRAWAL} SHIB.`, 400);
    }

    // 5. Deduct from balance
    const newBalance = user.balance - withdrawalAmount;
    await supabaseFetch('users', 'PATCH', { balance: newBalance }, `?id=eq.${id}`);

    // 6. Log the withdrawal request (assuming a 'withdrawals' table)
    await supabaseFetch('withdrawals', 'POST', {
      user_id: id,
      amount: withdrawalAmount,
      binance_id: binanceId,
      status: 'pending', // Initial status
      created_at: new Date().toISOString()
    });

    // 7. Update last_activity
    await supabaseFetch('users', 'PATCH', { last_activity: new Date().toISOString() }, `?id=eq.${id}&select=id`);

    // 8. Success
    sendSuccess(res, {
      new_balance: newBalance,
      message: 'Withdrawal request submitted successfully.'
    });

  } catch (error) {
    console.error('Withdraw failed:', error.message);
    sendError(res, `Failed to process withdrawal: ${error.message}`, 500);
  }
}

/**
 * 9) type: "requestActionId"
 */
async function handleRequestActionId(req, res, body) {
    const { user_id, action_type } = body;
    
    // Validate action_type to prevent abuse
    const allowedTypes = ['watchAd', 'preSpin', 'spinResult', 'withdraw', 'completeTask'];
    if (!allowedTypes.includes(action_type)) {
        return sendError(res, 'Invalid action type requested.', 400);
    }

    try {
        const actionId = await generateActionId(res, user_id, action_type);
        if (actionId) {
            sendSuccess(res, { action_id: actionId });
        }
    } catch (error) {
        // Error already sent by generateActionId
    }
}


/**
 * Main API entry point
 */
module.exports = async (req, res) => {
  // Only handle POST requests
  if (req.method !== 'POST') {
    return sendError(res, 'Method not allowed.', 405);
  }

  let body;
  try {
    body = await new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => {
        data += chunk.toString();
      });
      req.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('Invalid JSON payload.'));
        }
      });
      req.on('error', reject);
    });

  } catch (error) {
    return sendError(res, error.message, 400);
  }

  if (!body || !body.type) {
    return sendError(res, 'Missing "type" field in the request body.', 400);
  }

  // ⬅️ initData Security Check
  if (body.type !== 'commission' && (!body.initData || !validateInitData(body.initData))) {
      return sendError(res, 'Invalid or expired initData. Security check failed.', 401);
  }

  if (!body.user_id && body.type !== 'commission') {
      return sendError(res, 'Missing user_id in the request body.', 400);
  }

  // Route the request based on the 'type' field
  switch (body.type) {
    case 'getUserData':
      await handleGetUserData(req, res, body);
      break;
    case 'register':
      await handleRegister(req, res, body);
      break;
    case 'watchAd':
      await handleWatchAd(req, res, body);
      break;
    case 'commission':
      await handleCommission(req, res, body);
      break;
    case 'preSpin': 
      await handlePreSpin(req, res, body);
      break;
    case 'spinResult': 
      await handleSpinResult(req, res, body);
      break;
    case 'withdraw':
      await handleWithdraw(req, res, body);
      break;
    case 'completeTask': // ⬅️ THIS NOW HANDLES ALL MISSIONS VIA 'mission_id'
      await handleCompleteTask(req, res, body);
      break;
    case 'requestActionId':
      await handleRequestActionId(req, res, body);
      break;
    default:
      sendError(res, 'Invalid API request type.', 400);
  }
};