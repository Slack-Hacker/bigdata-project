// Function to show a message when button is clicked
function showMessage() {
    alert('Thanks for clicking! Your interaction has been logged. 😊');
}

// Function to send heartbeat to server to stay active
async function sendHeartbeat() {
    try {
        await fetch('/api/heartbeat', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
    } catch (error) {
        console.log('Heartbeat failed:', error);
    }
}

// Function to notify server when user is leaving
async function notifyUserLeaving() {
    try {
        await fetch('/api/user-leaving', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            keepalive: true // Important for when page is unloading
        });
    } catch (error) {
        console.log('User leaving notification failed:', error);  
    }
}

// Function to load and display visitor stats
async function loadStats() {
    try {
        const response = await fetch('/api/stats');
        const data = await response.json();
        
        const statsDiv = document.getElementById('stats');
        statsDiv.innerHTML = `
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px;">
                <div style="background: #e6fffa; padding: 20px; border-radius: 10px; border-left: 4px solid #38b2ac; text-align: center;">
                    <h3 style="margin: 0 0 10px 0; color: #2d3748;">🟢 Currently Active</h3>
                    <p style="font-size: 2rem; font-weight: bold; margin: 0; color: #38b2ac;">${data.current_active_users}</p>
                </div>
                <div style="background: #f0fff4; padding: 20px; border-radius: 10px; border-left: 4px solid #38a169; text-align: center;">
                    <h3 style="margin: 0 0 10px 0; color: #2d3748;">👥 Unique Users</h3>
                    <p style="font-size: 2rem; font-weight: bold; margin: 0; color: #38a169;">${data.total_unique_users}</p>
                </div>
                <div style="background: #fff5f5; padding: 20px; border-radius: 10px; border-left: 4px solid #e53e3e; text-align: center;">
                    <h3 style="margin: 0 0 10px 0; color: #2d3748;">📈 Total Visits</h3>
                    <p style="font-size: 2rem; font-weight: bold; margin: 0; color: #e53e3e;">${data.total_visits}</p>
                </div>
            </div>
            <div style="margin-top: 20px; text-align: center; color: #666;">
                <p><small>🕐 Last Updated: ${new Date().toLocaleTimeString()}</small></p>
                <p><small>💡 Opens terminal to see detailed user tracking</small></p>
            </div>
        `;
    } catch (error) {
        document.getElementById('stats').innerHTML = '<p>❌ Unable to load statistics</p>';
    }
}

// Load stats when page loads
document.addEventListener('DOMContentLoaded', function() {
    loadStats();
    
    // Refresh stats every 3 seconds for real-time active users
    setInterval(loadStats, 3000);
    
    // Send heartbeat every 5 seconds to stay active
    setInterval(sendHeartbeat, 5000);
    
    // Send initial heartbeat
    sendHeartbeat();
});

// Handle page visibility changes for immediate leave detection
document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
        // Page is hidden (tab switched, minimized, etc.)
        notifyUserLeaving();
        console.log('User left the page (tab hidden)');
    } else {
        // Page is visible again
        sendHeartbeat();
        loadStats(); // Refresh stats when user returns
        console.log('User returned to the page');
    }
});

// Handle page unload (browser close, navigation away)
window.addEventListener('beforeunload', function() {
    notifyUserLeaving();
});

// Handle page unload (backup method)
window.addEventListener('pagehide', function() {
    notifyUserLeaving();
});

// Fallback for page focus/blur
window.addEventListener('blur', function() {
    // Window lost focus
    setTimeout(() => {
        if (document.hidden) {
            notifyUserLeaving();
        }
    }, 100);
});

window.addEventListener('focus', function() {
    // Window gained focus
    sendHeartbeat();
});