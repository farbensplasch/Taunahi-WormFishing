import discord
from discord import app_commands
from discord.ui import Button, View, Select, Modal, TextInput
from discord.ext import commands
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from discord.ext import tasks
import asyncio
import time
from collections import deque
import aiohttp

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('rate_limiter')


def load_local_env(path: str = ".env") -> None:
    """Load environment variables from a local .env file if present."""
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_local_env()


def require_env_var(name: str, description: str) -> str:
    """Return the value for the required environment variable or raise a helpful error."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable {name} ({description}).")
    return value


def require_env_int(name: str, description: str, default: Optional[int] = None) -> int:
    """Parse an integer environment variable with a clear error message."""
    value = os.getenv(name)
    if value is None:
        if default is not None:
            return default
        raise RuntimeError(f"Missing environment variable {name} ({description}).")
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Environment variable {name} must be an integer ({description})."
        ) from exc


# Configuration
CONFIG = {
    "TARGET_CATEGORY_ID": require_env_int(
        "TARGET_CATEGORY_ID",
        "Discord category ID where party channels are created",
    ),
    "MAX_PLAYERS_PER_PARTY": require_env_int(
        "MAX_PLAYERS_PER_PARTY",
        "Maximum number of players allowed per party",
        default=6,
    ),
    "YOUR_CHANNEL_ID": require_env_int(
        "YOUR_CHANNEL_ID",
        "Channel ID where the Worm Party Finder message is posted",
    ),
    "AUTHORIZED_USER_ID": require_env_int(
        "AUTHORIZED_USER_ID",
        "Discord user ID permitted to manage the bot",
        default=407215852937805844,
    ),
    "MACRO_CHECKS_CHANNEL_ID": require_env_int(
        "MACRO_CHECKS_CHANNEL_ID",
        "Channel ID where macro checks are logged",
    ),
    "MAX_RETRIES": require_env_int(
        "MAX_RETRIES",
        "Maximum number of retries after rate limiting",
        default=3,
    ),
    "RETRY_DELAY": require_env_int(
        "RETRY_DELAY",
        "Base delay between rate limit retries (seconds)",
        default=5,
    ),
    "GUILD_ID": require_env_int(
        "GUILD_ID",
        "Primary guild ID where application commands are synced",
    ),
    "WORMFISHER_ROLE_ID": require_env_int(
        "WORMFISHER_ROLE_ID",
        "Role ID granted by the Wormfisher self-assign button",
    ),
    "SUPPORT_CONTACT": os.getenv(
        "SUPPORT_CONTACT",
        "farbensplasch",
    ),
    "SUPPORT_CONTACT_MENTION": os.getenv(
        "SUPPORT_CONTACT_MENTION",
        "<@407215852937805844>",
    ),
}


def get_bot_token() -> str:
    """Fetch the bot token from the environment with a descriptive error."""
    return require_env_var("DISCORD_BOT_TOKEN", "Discord bot token")


# Cached guild object for command syncs
GUILD_OBJECT = discord.Object(id=CONFIG["GUILD_ID"])


# Rate Limiter Class
class RateLimiter:
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests  # Maximum requests allowed in time window
        self.time_window = time_window    # Time window in seconds
        self.requests = deque(maxlen=max_requests)  # Use deque for efficient FIFO operations
        self.total_requests = 0           # Total requests made
        self.rate_limited_count = 0       # Number of times rate limited
        self.last_reset = time.time()     # Last time stats were reset
        self.start_time = time.time()     # When the rate limiter was started
        
    async def acquire(self):
        now = time.time()
        
        # Remove old requests outside the time window
        while self.requests and now - self.requests[0] >= self.time_window:
            self.requests.popleft()
        
        # If we've hit the rate limit, wait until we can make another request
        if len(self.requests) >= self.max_requests:
            sleep_time = self.requests[0] + self.time_window - now
            if sleep_time > 0:
                self.rate_limited_count += 1
                logger.warning(
                    f"Rate limit reached. Waiting {sleep_time:.2f}s. "
                    f"Current usage: {len(self.requests)}/{self.max_requests} requests in {self.time_window}s window"
                )
                await asyncio.sleep(sleep_time)
                return await self.acquire()
        
        # Add current request
        self.requests.append(now)
        self.total_requests += 1
        
        # Log request if we're close to the limit
        if len(self.requests) > self.max_requests * 0.8:  # 80% of limit
            logger.info(
                f"High request rate: {len(self.requests)}/{self.max_requests} requests in {self.time_window}s window"
            )
        
        return True
    
    def get_stats(self) -> dict:
        """Get current rate limiter statistics"""
        now = time.time()
        current_usage = len(self.requests)
        uptime = now - self.start_time
        requests_per_second = self.total_requests / uptime if uptime > 0 else 0
        
        return {
            "current_usage": current_usage,
            "max_requests": self.max_requests,
            "time_window": self.time_window,
            "total_requests": self.total_requests,
            "rate_limited_count": self.rate_limited_count,
            "requests_per_second": requests_per_second,
            "uptime_seconds": uptime
        }
    
    def reset_stats(self):
        """Reset statistics"""
        self.total_requests = 0
        self.rate_limited_count = 0
        self.last_reset = time.time()
        logger.info("Rate limiter statistics have been reset")

# Global rate limiter instance
rate_limiter = RateLimiter(max_requests=45, time_window=1.0)  # 45 requests per second

# Global state
class BotState:
    def __init__(self):
        self.active_channels: Dict[int, dict] = {}  # {channel_id: party_data}
        self.party_views: Dict[int, View] = {}
        self.user_participation: Dict[int, int] = {}  # {user_id: channel_id}
        self.initial_button_message_id: Optional[int] = None
        self.last_interaction_time: Dict[int, datetime] = {}  # {user_id: last_interaction_time}


state = BotState()

# Initialize bot
intents = discord.Intents.default()
intents.messages = True
intents.guilds = True
intents.members = True
intents.message_content = True

# Initialize bot with rate limit handling
class RateLimitAwareBot(commands.Bot):
    async def start(self, *args, **kwargs):
        retries = 0
        while retries < CONFIG["MAX_RETRIES"]:
            try:
                await super().start(*args, **kwargs)
                break
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limit error
                    retry_after = e.retry_after if hasattr(e, 'retry_after') else CONFIG["RETRY_DELAY"] * (2 ** retries)
                    logging.warning(f"Rate limited. Retrying in {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    retries += 1
                else:
                    raise
        else:
            raise Exception("Failed to start bot after maximum retries")

    async def http(self, *args, **kwargs):
        await rate_limiter.acquire()  # Wait for rate limit before making request
        return await super().http(*args, **kwargs)

# Use the rate limit aware bot
bot = RateLimitAwareBot(command_prefix='!', intents=intents)

# ====================== Worm Party Finder Components ======================

class CommandModal(Modal):
    def __init__(self, channel_id: int):
        super().__init__(title="Set Join Command")
        self.channel_id = channel_id
        self.command = TextInput(
            label="/wormparty join ... ...",
            placeholder="ex: /wormparty join Peach Test123",
            max_length=100
        )
        self.add_item(self.command)
    
    async def on_submit(self, interaction: discord.Interaction):
        state.active_channels[self.channel_id]['join_cmd'] = self.command.value
        await update_party_embed(self.channel_id)
        await post_initial_button()
        await interaction.response.send_message("Join command updated!", ephemeral=True)

class UsernameModal(Modal):
    def __init__(self):
        super().__init__(title="Ign")
        self.username = TextInput(
            label="Ign",
            placeholder="Ign...",
            min_length=3,
            max_length=16
        )
        self.add_item(self.username)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            if interaction.user.id in state.user_participation:
                channel_id = state.user_participation[interaction.user.id]
                channel = bot.get_channel(channel_id)
                try:
                    await interaction.response.send_message(
                        embed=discord.Embed(
                            title="Already in a Party",
                            description=f"You're already in a party! Please leave {channel.mention} before joining another.",
                            color=discord.Color.red()
                        ),
                        ephemeral=True
                    )
                except discord.NotFound:
                    # Interaction already timed out
                    return
                return

            try:
                await handle_party_join(interaction, self.username.value)
            except discord.NotFound:
                # Interaction already timed out
                return
            except Exception as e:
                logging.error(f"Error in UsernameModal on_submit: {e}")
                try:
                    if not interaction.response.is_done():
                        await interaction.response.send_message(
                            "An error occurred while processing your request.",
                            ephemeral=True
                        )
                except discord.NotFound:
                    # Interaction already timed out
                    return
        except Exception as e:
            logging.error(f"Unexpected error in UsernameModal on_submit: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An unexpected error occurred. Please try again.",
                        ephemeral=True
                    )
            except discord.NotFound:
                # Interaction already timed out
                return


class LockConfirmModal(Modal):
    def __init__(self, channel_id: int):
        super().__init__(title="Confirm AFK Party")
        self.channel_id = channel_id
        self.confirm = TextInput(
            label="Type 'AFK' to confirm",  # Shortened label
            placeholder="Party will be permanently locked (AFK)",
            max_length=3
        )
        self.add_item(self.confirm)
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.confirm.value.upper() != "AFK":
            await interaction.response.send_message(
                "You didn't type 'AFK'. Party remains unlocked.",
                ephemeral=True
            )
            return
            
        state.active_channels[self.channel_id]['locked'] = True
        await update_party_embed(self.channel_id)
        await post_initial_button()
        await interaction.response.send_message(
            "Party has been locked! No one can join now.",
            ephemeral=True
        )

class KickSelect(Select):
    def __init__(self, channel_id: int, members: List[int], usernames: List[str]):
        self.channel_id = channel_id
        options = []
        
        channel = bot.get_channel(channel_id)
        if not channel:
            raise ValueError("Channel not found")
            
        guild = channel.guild
        
        for member_id, mc_name in zip(members, usernames):
            member = guild.get_member(member_id)
            if member:
                options.append(discord.SelectOption(
                    label=f"{member.display_name} (MC: {mc_name})",
                    value=str(member_id)
                ))
        
        super().__init__(
            placeholder="Select a member to kick...",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        try:
            # Defer the interaction first to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            party_data = state.active_channels[self.channel_id]
            if interaction.user.id != party_data['creator_id']:
                await interaction.followup.send(
                    "Only the party creator can kick members!",
                    ephemeral=True
                )
                return
            
            member_id = int(self.values[0])
            if member_id == interaction.user.id:
                await interaction.followup.send(
                    "You can't kick yourself! Use the leave button instead.",
                    ephemeral=True
                )
                return
            
            # Remove the member from the party
            index = party_data['members'].index(member_id)
            party_data['members'].pop(index)
            mc_name = party_data['usernames'].pop(index)
            
            # Remove user from participation tracking
            if member_id in state.user_participation:
                del state.user_participation[member_id]
            
            channel = interaction.guild.get_channel(self.channel_id)
            member = interaction.guild.get_member(member_id)
            
            if member:
                await channel.set_permissions(
                    member,
                    read_messages=False,
                    send_messages=False
                )
                try:
                    await member.send(
                        embed=discord.Embed(
                            title="You were kicked from a Worm Party",
                            description=f"You were removed from the party in {channel.mention}",
                            color=discord.Color.red()
                        )
                    )
                except discord.Forbidden:
                    pass  # User has DMs disabled

            await update_party_embed(self.channel_id)
            await post_initial_button()

            # Send kick notification to the party channel
            await channel.send(
                f"{member.display_name if member else 'A member'} (MC: {mc_name}) was kicked by {interaction.user.mention}"
            )
            
                
        except Exception as e:
            logging.error(f"Error in KickSelect callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "An error occurred while processing your request.",
                    ephemeral=True
                )
            else:
                try:
                    await interaction.followup.send(
                        "An error occurred while processing your request.",
                        ephemeral=True
                    )
                except:
                    pass



class TransferLeaderSelect(Select):
    def __init__(self, channel_id: int, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Select new party leader...",
            min_values=1,
            max_values=1,
            options=options
        )
        self.channel_id = channel_id
    
    async def callback(self, interaction: discord.Interaction):
        try:
            # Defer the interaction first to prevent timeout
            await interaction.response.defer(ephemeral=True)
            
            party_data = state.active_channels.get(self.channel_id)
            if not party_data:
                await interaction.followup.send("Party data not found!", ephemeral=True)
                return
                
            if interaction.user.id != party_data['creator_id']:
                await interaction.followup.send(
                    "Only the current party leader can transfer leadership!",
                    ephemeral=True
                )
                return
                
            new_leader_id = int(self.values[0])
            
            # Update the creator_id in party data
            old_leader_id = party_data['creator_id']
            party_data['creator_id'] = new_leader_id
            
            # Reset votekick votes when leader changes
            party_data['votekick_votes'] = set()

            # Get member objects for notifications
            channel = interaction.guild.get_channel(self.channel_id)
            old_leader = interaction.guild.get_member(old_leader_id)
            new_leader = interaction.guild.get_member(new_leader_id)
            
            # Notify the party
            if channel:
                await channel.send(
                    f"{new_leader.mention if new_leader else 'The new leader'} is now the party leader! "
                    f"They can now set the join command and manage the party."
                )
            
            # Update the embed with new leader controls
            await update_party_embed(self.channel_id)
            await post_initial_button()
            
            
        except Exception as e:
            logging.error(f"Error in TransferLeaderSelect callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "An error occurred while transferring leadership.",
                    ephemeral=True
                )
            else:
                try:
                    await interaction.followup.send(
                        "An error occurred while transferring leadership.",
                        ephemeral=True
                    )
                except:
                    pass



class SizeSelectView(View):
    def __init__(self, channel_id: int, current_size: int, creator_id: int):
        super().__init__(timeout=30)
        self.creator_id = creator_id
        options = []
        
        for size in range(2, CONFIG["MAX_PLAYERS_PER_PARTY"] + 1):
            options.append(discord.SelectOption(
                label=f"{size} players",
                value=str(size),
                default=(size == current_size)
            ))
        
        select = Select(
            placeholder="Select maximum party size...",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"size_select_{channel_id}"
        )
        select.callback = self.on_select
        self.add_item(select)
    
    async def on_select(self, interaction: discord.Interaction):
        try:
            # Check if the user is the party creator
            if interaction.user.id != self.creator_id:
                await interaction.response.send_message(
                    "Only the party creator can adjust the party size!",
                    ephemeral=True
                )
                return
                
            channel_id = int(interaction.data['custom_id'].split('_')[-1])
            new_size = int(interaction.data['values'][0])
        
            # Defer the interaction first
            await interaction.response.defer()
        
            state.active_channels[channel_id]['max_size'] = new_size
            await update_party_embed(channel_id)
            await post_initial_button()
        
            channel = bot.get_channel(channel_id)
            await channel.send(f"Party size changed to {new_size} players by {interaction.user.mention}")
        
            # Edit the original response to show success
            await interaction.followup.send(
                content=f"Party size updated to {new_size}",
                ephemeral=True
            )
        
        except Exception as e:
            logging.error(f"Error in SizeSelectView: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "An error occurred while changing party size",
                    ephemeral=True
                )

class PartyView(View):
    def __init__(self, channel_id: int, creator_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id
        self.creator_id = creator_id
        
        party_data = state.active_channels.get(channel_id, {})
        is_creator = party_data.get('creator_id') == creator_id
        
        # Set Join CMD button (only for creator)
        if is_creator:
            cmd_button = Button(
                label="Set Join CMD",
                style=discord.ButtonStyle.blurple,
                emoji="💻",
                custom_id=f"cmd_button_{channel_id}"
            )
            cmd_button.callback = self.on_cmd_button
            self.add_item(cmd_button)
        
        # Transfer Leader button (only for creator, only if there are other members)
        if is_creator and len(party_data.get('members', [])) > 1:
            transfer_button = Button(
                label="Transfer Leader",
                style=discord.ButtonStyle.grey,
                emoji="👑",
                custom_id=f"transfer_button_{channel_id}"
            )
            transfer_button.callback = self.on_transfer_button
            self.add_item(transfer_button)

        # Adjust size button (only for creator)
        if is_creator:
            size_button = Button(
                label="Adjust Size",
                style=discord.ButtonStyle.grey,
                emoji="🔢",
                custom_id=f"size_button_{channel_id}"
            )
            size_button.callback = self.on_size_button
            self.add_item(size_button)
        
        # Leave button for everyone
        leave_button = Button(
            label="Leave Party",
            style=discord.ButtonStyle.red,
            emoji="🚪",
            custom_id=f"leave_party_{channel_id}"
        )
        leave_button.callback = self.on_leave_button
        self.add_item(leave_button)
        
        # Kick button (only for creator)
        if is_creator:
            kick_button = Button(
                label="Kick Member",
                style=discord.ButtonStyle.red,
                emoji="👢",
                custom_id=f"kick_button_{channel_id}"
            )
            kick_button.callback = self.on_kick_button
            self.add_item(kick_button)
        
        # Lock button (only for creator, only if not already locked)
        if is_creator and not party_data.get('locked', False):
            lock_button = Button(
                label="AFK Party",
                style=discord.ButtonStyle.danger,
                emoji="🔒",
                custom_id=f"lock_button_{channel_id}"
            )
            lock_button.callback = self.on_lock_button
            self.add_item(lock_button)

        # Vote Kick button: always enabled, but leader is denied in callback
        votekick_button = Button(
            label="Vote Kick Leader",
            style=discord.ButtonStyle.danger,
            emoji="⚡",
            custom_id=f"votekick_button_{channel_id}",
            disabled=False
        )
        votekick_button.callback = self.on_votekick_button
        self.add_item(votekick_button)

    async def on_leave_button(self, interaction: discord.Interaction):
        if interaction.user.id not in state.active_channels[self.channel_id]['members']:
            await interaction.response.send_message("You're not in this Party!", ephemeral=True)
            return
            
        index = state.active_channels[self.channel_id]['members'].index(interaction.user.id)
        state.active_channels[self.channel_id]['members'].pop(index)
        state.active_channels[self.channel_id]['usernames'].pop(index)
        
        # Remove user from participation tracking
        if interaction.user.id in state.user_participation:
            del state.user_participation[interaction.user.id]
        
        channel = interaction.guild.get_channel(self.channel_id)
        await channel.set_permissions(
            interaction.user,
            read_messages=False,
            send_messages=False
        )
        
        # Get the party data and creator
        party_data = state.active_channels[self.channel_id]
        creator = interaction.guild.get_member(party_data['creator_id'])
        
        # Check if the leaving user was the creator
        if interaction.user.id == party_data['creator_id'] and party_data['members']:
            # Transfer creator role to the oldest remaining member
            new_creator_id = party_data['members'][0]
            party_data['creator_id'] = new_creator_id
            
            # Reset votekick votes when leader changes
            party_data['votekick_votes'] = set()
            
            # Notify the Party about the change
            new_creator = interaction.guild.get_member(new_creator_id)
            await channel.send(
                f"Party creator has left. {new_creator.mention} is now the new Party creator "
                "and can set the join command."
            )
            creator = new_creator  # Update creator reference for the leave message
        
        # Get the updated member count
        member_count = len(party_data['members'])
        
        # Create the appropriate trigger message based on member count
        trigger_messages = {
            1: "1 Member = 15-20",
            2: "2 Member = 25-30",
            3: "3 Member = 35-40",
            4: "4 Member = 45-50",
            5: "5 Member = 50-55",
            6: "6 Member = 55-60"
        }
        
        # Send leave notification if there are still members left
        if member_count > 0 and member_count in trigger_messages:
            leave_embed = discord.Embed(
                title=f"Player Left the Party",
                description=f"{interaction.user.mention} has left the party!",
                color=discord.Color.orange()
            )
            leave_embed.add_field(
                name="Updated Worm Trigger Count",
                value=trigger_messages[member_count],
                inline=False
            )
            leave_embed.set_footer(text=f"Party Creator: {creator.display_name if creator else 'Unknown'}")
            await channel.send(embed=leave_embed)

        await update_party_embed(self.channel_id)
        await post_initial_button()
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Left Party",
                description="You've left the Party.",
                color=discord.Color.blue()
            ),
            ephemeral=True
        )
        
        # Delete channel if empty
        if not party_data['members']:
            await channel.delete()
            del state.active_channels[self.channel_id]
            if self.channel_id in state.party_views:
                del state.party_views[self.channel_id]
            await post_initial_button()
    

    async def on_transfer_button(self, interaction: discord.Interaction):
        party_data = state.active_channels.get(self.channel_id)
        if not party_data:
            await interaction.response.send_message("Party data not found!", ephemeral=True)
            return
        
        if interaction.user.id != party_data['creator_id']:
            await interaction.response.send_message(
                "Only the party leader can transfer leadership!",
                ephemeral=True
            )
            return
        
        # Create a select menu of members to transfer to (excluding current leader)
        options = []
        for member_id, mc_name in zip(party_data['members'], party_data['usernames']):
            if member_id != party_data['creator_id']:  # Don't include current leader
                member = interaction.guild.get_member(member_id)
                if member:
                    options.append(discord.SelectOption(
                        label=f"{member.display_name} (MC: {mc_name})",
                        value=str(member_id)
                    ))
    
        if not options:
            await interaction.response.send_message(
                "There are no other members to transfer leadership to!",
                ephemeral=True
            )
            return
        
        view = View(timeout=30)
        view.add_item(TransferLeaderSelect(self.channel_id, options))
    
        await interaction.response.send_message(
            "Select the new party leader:",
            view=view,
        )



    async def on_cmd_button(self, interaction: discord.Interaction):
        if interaction.user.id != state.active_channels[self.channel_id]['creator_id']:
            await interaction.response.send_message(
                "Only the Party creator can set the join command!",
                ephemeral=True
            )
            return
        
        await interaction.response.send_modal(CommandModal(self.channel_id))


    async def on_size_button(self, interaction: discord.Interaction):
        party_data = state.active_channels[self.channel_id]
        if interaction.user.id != party_data['creator_id']:
            await interaction.response.send_message(
                "Only the party creator can adjust the party size!",
                ephemeral=True
            )
            return

        current_size = party_data['max_size']
        current_members = len(party_data['members'])

        if current_members > current_size:
            await interaction.response.send_message(
                f"You currently have {current_members} members which is more than your set maximum ({current_size}). "
                "You can't reduce the size below your current member count.",
                ephemeral=True
            )
            return

        # Defer before sending the view

        await interaction.response.defer(ephemeral=False)
        view = SizeSelectView(self.channel_id, current_size, party_data['creator_id'])
        await interaction.followup.send(
            "Select the new maximum party size:",
            view=view
        )


    async def on_kick_button(self, interaction: discord.Interaction):
        party_data = state.active_channels[self.channel_id]
        if interaction.user.id != party_data['creator_id']:
            await interaction.response.send_message(
                "Only the party creator can kick members!",
                ephemeral=True
            )
            return

        # Don't allow kicking if there's only 1 member
        if len(party_data['members']) <= 1:
            await interaction.response.send_message(
                "You can't kick yourself! Use the leave button instead.",
                ephemeral=True
            )
            return

        # Create a view with select menu of members to kick
        view = View(timeout=30)
        view.add_item(KickSelect(
            self.channel_id,
            party_data['members'],
            party_data['usernames']
        ))

        # Send the response with the view
        await interaction.response.send_message(
            "Select a member to kick:",
            view=view,
            ephemeral=False
        )
    

    
    async def on_lock_button(self, interaction: discord.Interaction):
        if interaction.user.id != state.active_channels[self.channel_id]['creator_id']:
            await interaction.response.send_message(
                "Only the Party creator can lock the party!",
                ephemeral=True
            )
            return
            
        await interaction.response.send_modal(LockConfirmModal(self.channel_id))

    async def on_votekick_button(self, interaction: discord.Interaction):
        try:
            party_data = state.active_channels.get(self.channel_id)
            if not party_data:
                await interaction.response.send_message("Party data not found!", ephemeral=True)
                return

            # Check if user is in the party
            if interaction.user.id not in party_data['members']:
                await interaction.response.send_message("You must be in the party to vote kick!", ephemeral=True)
                return

            # Check if user is the creator
            if interaction.user.id == party_data['creator_id']:
                await interaction.response.send_message("You cannot vote kick yourself!", ephemeral=True)
                return

            # Initialize vote tracking if not exists
            if 'votekick_votes' not in party_data:
                party_data['votekick_votes'] = set()

            # Check if user has already voted
            if interaction.user.id in party_data['votekick_votes']:
                await interaction.response.send_message("You have already voted to kick the leader!", ephemeral=True)
                return

            # Add vote
            party_data['votekick_votes'].add(interaction.user.id)

            # Calculate required votes (all non-leader members)
            non_creator_members = [m for m in party_data['members'] if m != party_data['creator_id']]
            required_votes = len(non_creator_members)

            # Get current vote count
            current_votes = len(party_data['votekick_votes'])

            # Send vote confirmation
            await interaction.response.send_message(
                f"Vote recorded! {current_votes}/{required_votes} votes needed to kick the leader.",
                ephemeral=True
            )

            # If this is the first vote, announce in the party channel
            if current_votes == 1:
                channel = interaction.guild.get_channel(self.channel_id)
                await channel.send(f"Vote to kick the party leader started! ({current_votes}/{required_votes} votes)")

            # Check if enough votes (unanimous from all non-leader members)
            if current_votes >= required_votes:
                channel = interaction.guild.get_channel(self.channel_id)
                creator = interaction.guild.get_member(party_data['creator_id'])

                # Notify the party
                await channel.send(
                    f"Vote kick successful! {creator.mention} has been removed from the party due to inactivity."
                )

                # Remove creator from party
                index = party_data['members'].index(party_data['creator_id'])
                party_data['members'].pop(index)
                party_data['usernames'].pop(index)

                # Remove creator from participation tracking
                if party_data['creator_id'] in state.user_participation:
                    del state.user_participation[party_data['creator_id']]

                # Update permissions
                await channel.set_permissions(
                    creator,
                    read_messages=False,
                    send_messages=False
                )

                # Transfer leadership to the oldest remaining member
                if party_data['members']:
                    new_creator_id = party_data['members'][0]
                    party_data['creator_id'] = new_creator_id
                    new_creator = interaction.guild.get_member(new_creator_id)
                    await channel.send(
                        f"{new_creator.mention} is now the new party leader!"
                    )

                # Reset votekick votes
                party_data['votekick_votes'] = set()

                # Update the party embed
                await update_party_embed(self.channel_id)
                await post_initial_button()

        except Exception as e:
            logging.error(f"Error in on_votekick_button: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while processing your vote.",
                        ephemeral=True
                    )
            except discord.NotFound:
                pass

async def update_party_embed(channel_id: int):
    channel = bot.get_channel(channel_id)
    if not channel or channel_id not in state.active_channels:
        return
    
    data = state.active_channels[channel_id]
    try:
        message = await channel.fetch_message(data['message_id'])
        
        embed = discord.Embed(
            title=f"⚔️ Worm Party #{list(state.active_channels.keys()).index(channel_id)+1}",
            color=discord.Color.green()
        )
        
        # Add locked status to title if party is locked
        if data.get('locked', False):
            embed.title += " (LOCKED 🔒)"
        
        # Players list
        player_list = []
        for i, (member_id, mc_name) in enumerate(zip(data['members'], data['usernames']), 1):
            member = channel.guild.get_member(member_id)
            player_list.append(f"{i}. {member.mention} (MC: {mc_name})")
        
        embed.add_field(
            name="Players",
            value="\n".join(player_list) or "No players in Party",
            inline=False
        )
        
        # Join command (if set)
        if data.get('join_cmd'):
            embed.add_field(
                name="Join Command",
                value=f"```{data['join_cmd']}```",
                inline=False
            )
        
        player_count = len(data['members'])
        max_size = data['max_size']
        status = "Party complete! 🎉" if player_count == max_size else f"{player_count}/{max_size} players joined"
        
        # Add locked status to footer if party is locked
        if data.get('locked', False):
            status += " | PARTY LOCKED 🔒"
        
        embed.set_footer(text=status)
        
        view = PartyView(channel_id, data['creator_id'])
        await message.edit(embed=embed, view=view)
        
        # Pin the message if it's not already pinned
        if not message.pinned:
            try:
                await message.pin()
            except (discord.Forbidden, discord.HTTPException) as e:
                logging.warning(f"Failed to pin message: {e}")
        
    except discord.NotFound:
        logging.warning(f"Message not found for channel {channel_id}")

async def handle_party_join(interaction: discord.Interaction, mc_username: str):
    # First check if user is trying to join a locked party
    for ch_id, data in state.active_channels.items():
        if interaction.user.id in data['members'] and data.get('locked', False):
            channel = bot.get_channel(ch_id)
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Party Locked",
                    description=f"The party in {channel.mention} is locked and not accepting new members.",
                    color=discord.Color.red()
                ),
                ephemeral=True
            )
            return
    
    category = interaction.guild.get_channel(CONFIG["TARGET_CATEGORY_ID"])
    if not category:
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Error",
                description="Couldn't find the Wormparty category!",
                color=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    channel = None
    for ch_id, data in state.active_channels.items():
        if len(data['members']) < data['max_size'] and not data.get('locked', False):
            channel = interaction.guild.get_channel(ch_id)
            break

    if channel is None:
        # Create new party channel
        overwrites = {
            interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            interaction.user: discord.PermissionOverwrite(read_messages=True)
        }
        channel = await category.create_text_channel(
            f'Worm-Party-{len(state.active_channels)+1}',
            overwrites=overwrites
        )
        
        state.active_channels[channel.id] = {
            'members': [interaction.user.id],
            'usernames': [mc_username],
            'message_id': None,
            'creator_id': interaction.user.id,
            'join_cmd': None,
            'max_size': CONFIG["MAX_PLAYERS_PER_PARTY"],
            'locked': False
        }

        # Track user participation
        state.user_participation[interaction.user.id] = channel.id
        await post_initial_button()

        view = PartyView(channel.id, interaction.user.id)
        state.party_views[channel.id] = view

        embed = discord.Embed(
            title=f"⚔️ Wormparty #{len(state.active_channels)}",
            color=discord.Color.green()
        )
        embed.add_field(
            name="Players",
            value=f"1. {interaction.user.mention} (MC: {mc_username})",
            inline=False
        )
        embed.set_footer(text=f"1/{CONFIG['MAX_PLAYERS_PER_PARTY']} players joined")
        
        message = await channel.send(embed=embed, view=view)
        state.active_channels[channel.id]['message_id'] = message.id

        try:
            await message.pin()
        except (discord.Forbidden, discord.HTTPException) as e:
            logging.warning(f"Failed to pin message: {e}")

        await interaction.response.send_message(
            embed=discord.Embed(
                title="Party Created!",
                description=f"You've started a new Party in {channel.mention}",
                color=discord.Color.green()
            ),
            ephemeral=True
        )
    else:
        # Join existing party
        state.active_channels[channel.id]['members'].append(interaction.user.id)
        state.active_channels[channel.id]['usernames'].append(mc_username)
        state.user_participation[interaction.user.id] = channel.id
        
        await channel.set_permissions(
            interaction.user,
            read_messages=True,
            send_messages=True
        )

        # Get the current member count
        member_count = len(state.active_channels[channel.id]['members'])
        
        # Create the appropriate trigger message based on member count
        trigger_messages = {
            1: "1 Member = 15-20",
            2: "2 Member = 25-30",
            3: "3 Member = 35-40",
            4: "4 Member = 45-50",
            5: "5 Member = 50-55",
            6: "6 Member = 55-60"
        }
        
        if member_count in trigger_messages:
            join_embed = discord.Embed(
                title=f"New Player Joined!",
                description=f"{interaction.user.mention} has joined the party!",
                color=discord.Color.green()
            )
            join_embed.add_field(
                name="Worm Trigger Count",
                value=trigger_messages[member_count],
                inline=False
            )
            await channel.send(embed=join_embed)

        await update_party_embed(channel.id)
        await post_initial_button()
        await interaction.response.send_message(
            embed=discord.Embed(
                title="Joined Party!",
                description=f"You've joined {channel.mention}",
                color=discord.Color.green()
            ),
            ephemeral=True
        )
        
        if len(state.active_channels[channel.id]['members']) == CONFIG["MAX_PLAYERS_PER_PARTY"]:
            await channel.send("Your Party is full!")

async def on_join_button(interaction: discord.Interaction):
    try:
        # Rate limiting - 5 seconds between interactions per user
        cooldown = timedelta(seconds=5)
        last_interaction = state.last_interaction_time.get(interaction.user.id)
        
        if last_interaction and (datetime.now() - last_interaction) < cooldown:
            remaining = (cooldown - (datetime.now() - last_interaction)).seconds
            await interaction.response.send_message(
                f"Please wait {remaining} seconds before using this again.",
                ephemeral=True
            )
            return
        
        state.last_interaction_time[interaction.user.id] = datetime.now()

        if interaction.user.id in state.user_participation:
            channel_id = state.user_participation[interaction.user.id]
            channel = bot.get_channel(channel_id)
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="Already in a Party",
                    description=f"You're already in a party! Please leave {channel.mention} before joining another.",
                    color=discord.Color.red()
                ),
                ephemeral=True
            )
            return
        
        # Create and show the modal
        modal = UsernameModal()
        await interaction.response.send_modal(modal)
        
    except Exception as e:
        logging.error(f"Error in on_join_button: {e}")
        if not interaction.response.is_done():
            await interaction.response.send_message(
                "An error occurred while processing your request. Please try again.",
                ephemeral=True
            )

async def post_initial_button():
    channel = bot.get_channel(CONFIG["YOUR_CHANNEL_ID"])
    if not channel:
        return

    try:
        if state.initial_button_message_id:
            message = await channel.fetch_message(state.initial_button_message_id)
        else:
            # Search for the last message sent by the bot in this channel
            found_message = None
            async for msg in channel.history(limit=10):
                if msg.author == bot.user and msg.embeds and "Worm Party Finder" in msg.embeds[0].title:
                    found_message = msg
                    break
            
            if found_message:
                message = found_message
                state.initial_button_message_id = found_message.id
            else:
                message = None
        
        if message:
            # Create party list
            party_list = []
            for ch_id, data in state.active_channels.items():
                channel = bot.get_channel(ch_id)
                if channel:
                    member_count = len(data['members'])
                    max_size = data['max_size']
                    afk_status = "🔒 AFK" if data.get('locked', False) else ""
                    party_list.append(f"• {channel.mention} - {member_count}/{max_size} players {afk_status}")
            
            # Update existing message
            embed = discord.Embed(
                title="Worm Party Finder",
                description=f"Click the button to create a Worm party.\n\n**Current active parties:** {len(state.active_channels)}",
                color=discord.Color.blue()
            )
            
            if party_list:
                embed.add_field(
                    name="Active Parties",
                    value="\n".join(party_list),
                    inline=False
                )
            else:
                embed.add_field(
                    name="Active Parties",
                    value="No active parties at the moment",
                    inline=False
                )
            
            view = View(timeout=None)
            join_button = Button(
                label="Join / Create a Worm Party", 
                style=discord.ButtonStyle.green,
                emoji="⛏️",
                custom_id="initial_join_button"
            )
            join_button.callback = on_join_button
            view.add_item(join_button)
            await message.edit(embed=embed, view=view)
            return
        
    except discord.NotFound:
        pass  # Message doesn't exist, will create new one
    except Exception as e:
        logging.error(f"Error while trying to find/edit message: {e}")
    
    # Create new message if we couldn't find an existing one
    # Create party list
    party_list = []
    for ch_id, data in state.active_channels.items():
        channel = bot.get_channel(ch_id)
        if channel:
            member_count = len(data['members'])
            max_size = data['max_size']
            afk_status = "🔒 AFK" if data.get('locked', False) else ""
            party_list.append(f"• {channel.mention} - {member_count}/{max_size} players {afk_status}")
    
    embed = discord.Embed(
        title="Worm Party Finder",
        description=f"Click the button to create a Worm party.\n\n**Current active parties:** {len(state.active_channels)}",
        color=discord.Color.blue()
    )
    
    if party_list:
        embed.add_field(
            name="Active Parties",
            value="\n".join(party_list),
            inline=False
        )
    else:
        embed.add_field(
            name="Active Parties",
            value="No active parties at the moment",
            inline=False
        )
    
    view = View(timeout=None)
    join_button = Button(
        label="Join / Create a Worm Party", 
        style=discord.ButtonStyle.green,
        emoji="⛏️",
        custom_id="initial_join_button"
    )
    join_button.callback = on_join_button
    view.add_item(join_button)
    message = await channel.send(embed=embed, view=view)
    state.initial_button_message_id = message.id

# ====================== Wormfishing Guide Components ======================

class MenuView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(Button(style=discord.ButtonStyle.primary, label="Taunahi Settings", custom_id="taunahi_settings"))
        self.add_item(Button(style=discord.ButtonStyle.success, label="3rd Party Mods", custom_id="third_party_mods"))
        self.add_item(Button(style=discord.ButtonStyle.danger, label="Ingame Setup", custom_id="ingame_setup"))

class ThirdPartyModsView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(Button(style=discord.ButtonStyle.primary, label="Odin", custom_id="odin_mod"))
        self.add_item(Button(style=discord.ButtonStyle.secondary, label="Chattriggers", custom_id="chattriggers_mod"))
        self.add_item(Button(style=discord.ButtonStyle.success, label="NEU", custom_id="neu_mod"))

class IngameSetupView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(Button(style=discord.ButtonStyle.primary, label="Fishing Setup", custom_id="fishing_setup"))
        self.add_item(Button(style=discord.ButtonStyle.secondary, label="Cage Setup", custom_id="cage_setup"))

class FishingSetupView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(Button(emoji="🛡️", label="Armor", custom_id="fishing_armor", style=discord.ButtonStyle.primary))
        self.add_item(Button(emoji="🧰", label="Equipment", custom_id="fishing_equipment", style=discord.ButtonStyle.primary))
        self.add_item(Button(emoji="🐚", label="Pet", custom_id="fishing_pet", style=discord.ButtonStyle.primary))
        self.add_item(Button(emoji="🎣", label="Fishing Rod", custom_id="fishing_rod", style=discord.ButtonStyle.primary))
        self.add_item(Button(emoji="⚔️", label="Weapons", custom_id="fishing_weapons", style=discord.ButtonStyle.primary))
        self.add_item(Button(emoji="🪨", label="Power Stone", custom_id="power_stone", style=discord.ButtonStyle.primary))
        self.add_item(Button(emoji="⛏️", label="HOTM", custom_id="fishing_hotm", style=discord.ButtonStyle.primary))

class RoleView(View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(Button(style=discord.ButtonStyle.primary, label="Wormfisher", custom_id="role_wormfisher", emoji="🎣"))

# ====================== Bot Events and Commands ======================

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    await post_initial_button()
    logging.info(f'Logged in as {bot.user} (ID: {bot.user.id})')
    
    # Add persistent views
    bot.add_view(PartyView(0, 0))  # For the initial button
    
    logging.info("Syncing commands...")
    try:
        # Sync commands for the current guild
        synced = await bot.tree.sync(guild=GUILD_OBJECT)
        logging.info(f"Successfully synced {len(synced)} commands")
    except Exception as e:
        logging.error(f"Error syncing commands: {e}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
        
    if message.content == "!menu18769":
        embed = discord.Embed(
            title="Wormfishing Guide",
            description=(
                "**This bot is still in development!** 🚧\n"
                f"Report bugs, incorrect information, or suggestions to {CONFIG['SUPPORT_CONTACT_MENTION']} "
                f"({CONFIG['SUPPORT_CONTACT']})"
            ),
            color=discord.Color.blue()
        )
        await message.channel.send(embed=embed, view=MenuView())
    
    elif message.content == "!Role18769":
        embed = discord.Embed(
            title="Self-Role Selection",
            description="Click the button below to get the Wormer role:\n\n"
                      "🎣 **Wormer** - For those who want to get pinged",
            color=discord.Color.blue()
        )
        await message.channel.send(embed=embed, view=RoleView())
    
    # Add the !close command handler
    elif message.content == "!close":
        # Check if the message author is the bot creator
        if message.author.id != CONFIG["AUTHORIZED_USER_ID"]:
            await message.channel.send(
                embed=discord.Embed(
                    title="Permission Denied",
                    description="Only the bot creator can use this command!",
                    color=discord.Color.red()
                )
            )
            return
            
        # Check if the channel is a party channel
        if message.channel.id not in state.active_channels:
            await message.channel.send(
                embed=discord.Embed(
                    title="Error",
                    description="This is not an active party channel!",
                    color=discord.Color.red()
                )
            )
            return
            
        # Get party data
        party_data = state.active_channels[message.channel.id]
        
        # Notify all members
        for member_id in party_data['members']:
            member = message.guild.get_member(member_id)
            if member:
                try:
                    await member.send(
                        embed=discord.Embed(
                            title="Party Closed",
                            description=f"The party in {message.channel.mention} was closed by the bot creator.",
                            color=discord.Color.red()
                        )
                    )
                except discord.Forbidden:
                    pass  # User has DMs disabled
            
            # Remove user from participation tracking
            if member_id in state.user_participation:
                del state.user_participation[member_id]
        
        # Delete the channel
        await message.channel.delete()
        
        # Clean up state
        del state.active_channels[message.channel.id]
        if message.channel.id in state.party_views:
            del state.party_views[message.channel.id]
            
        await post_initial_button()

@bot.event
async def on_interaction(interaction):
    if interaction.type != discord.InteractionType.component:
        return
    
    try:
        custom_id = interaction.data.get("custom_id")
        logging.info(f"Received interaction with custom_id: {custom_id}")
        
        # Handle initial join button
        if custom_id == "initial_join_button":
            logging.info("Handling initial join button interaction")
            try:
                await on_join_button(interaction)
                logging.info("Successfully handled initial join button")
            except Exception as e:
                logging.error(f"Error in on_join_button: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while processing your request. Please try again.",
                        ephemeral=True
                    )
            return
            
        # Role selection interactions
        if custom_id == "role_wormfisher":
            role_id = CONFIG["WORMFISHER_ROLE_ID"]
            
            # Check if bot has manage roles permission
            if not interaction.guild.me.guild_permissions.manage_roles:
                await interaction.response.send_message(
                    "I don't have permission to manage roles. Please ask an administrator to give me the 'Manage Roles' permission.",
                    ephemeral=True
                )
                return
            
            role = interaction.guild.get_role(role_id)
            if not role:
                await interaction.response.send_message("Role not found! Please contact an administrator.", ephemeral=True)
                return
            
            # Check if bot's highest role is above the role it's trying to manage
            if role.position >= interaction.guild.me.top_role.position:
                await interaction.response.send_message(
                    "I can't manage this role because it's higher than or equal to my highest role. Please ask an administrator to move my role above this role.",
                    ephemeral=True
                )
                return
            
            try:
                if role in interaction.user.roles:
                    await interaction.user.remove_roles(role)
                    await interaction.response.send_message(f"Removed {role.name} role!", ephemeral=True)
                else:
                    await interaction.user.add_roles(role)
                    await interaction.response.send_message(f"Added {role.name} role!", ephemeral=True)
            except discord.Forbidden:
                await interaction.response.send_message(
                    "I don't have permission to manage this role. Please ask an administrator to check my permissions.",
                    ephemeral=True
                )
            return
        
        # Wormfishing guide interactions
        elif custom_id == "taunahi_settings":
            embed1 = discord.Embed(
                title="Taunahi Settings Guide",
                description=" ",
                color=discord.Color.blue()
            )
            embed1.add_field(name="Worm Trigger Count:", value="1 Member = 15-20", inline=False)
            embed1.add_field(name="", value="2 Member = 25-30", inline=False)
            embed1.add_field(name="", value="3 Member = 35-40", inline=False)
            embed1.add_field(name="", value="4 Member = 45-50", inline=False)
            embed1.add_field(name="", value="5 Member = 50-55", inline=False)
            embed1.add_field(name="", value="6 Member = 55-60", inline=False)
            embed1.add_field(name="Only important for the Killer / Party creator", value=" ", inline=False)
            
            embed2 = discord.Embed(color=discord.Color.blue())
            embed2.set_image(url="https://cdn.discordapp.com/attachments/1373747930066063472/1373834661033414829/Screenshot_2025-05-19_030542.png")
            
            embed3 = discord.Embed(color=discord.Color.blue())
            embed3.set_image(url="https://cdn.discordapp.com/attachments/1373747930066063472/1373834661830197298/Screenshot_2025-05-19_030627.png")
            
            embed4 = discord.Embed(color=discord.Color.blue())
            embed4.set_image(url="https://cdn.discordapp.com/attachments/1373747930066063472/1373834661435805746/Screenshot_2025-05-19_030649.png")
            
            await interaction.response.send_message(embeds=[embed1, embed2, embed3, embed4], ephemeral=True)
        
        elif custom_id == "third_party_mods":
            embed = discord.Embed(
                title="3rd Party Mods",
                description="Select a mod to view more information:",
                color=discord.Color.green()
            )
            await interaction.response.send_message(embed=embed, view=ThirdPartyModsView(), ephemeral=True)
        
        elif custom_id == "ingame_setup":
            embed = discord.Embed(
                title="Ingame Setup",
                description="Select a setup type:",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=embed, view=IngameSetupView(), ephemeral=True)
        
        elif custom_id == "fishing_setup":
            embed = discord.Embed(
                title="Fishing Setup",
                description="Select a category to view the recommended setup:",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed, view=FishingSetupView(), ephemeral=True)
        
        elif custom_id == "fishing_armor":
            embed1 = discord.Embed(
                title="Magma Lord Helmet 9✪",
                description=" ",
                color=discord.Color.orange()
            )
            embed1.add_field(name="***Upgrades:***", value="**Reforge:**  Festive\n**Modifiers:**     Recombulator 3000\n**Gemstones:**   2x Perfect Aqua\n**Enchants:**  Bobbin' Time V\n**Attributes:** Fishing Experience 10", inline=True)
            embed1.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374148870249910342/magma_lord_helmet.png")
            
            embed2 = discord.Embed(
                title="Magma Lord Chestplate 9✪",
                description="",
                color=discord.Color.orange()
            )
            embed2.add_field(name="***Upgrades:***", value="**Reforge:**  Festive\n**Modifiers:**     Recombulator 3000\n**Gemstones:**   2x Perfect Aqua\n**Enchants:**  Bobbin' Time V\n**Attributes:** Fishing Experience 10", inline=True)
            embed2.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374149395939065916/magma_lord_chestplate.png")
            
            embed3 = discord.Embed(
                title="Magma Lord Leggings 9✪",
                description="",
                color=discord.Color.orange()
            )
            embed3.add_field(name="***Upgrades:***", value="**Reforge:**  Festive\n**Modifiers:**     Recombulator 3000\n**Gemstones:**   2x Perfect Aqua\n**Enchants:**  Bobbin' Time V\n**Attributes:** Fishing Experience 10", inline=True)
            embed3.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374149434518143066/magma_lord_leggings.png")
            
            embed4 = discord.Embed(
                title="Magma Lord Boots 9✪",
                description="",
                color=discord.Color.orange()
            )
            embed4.add_field(name="***Upgrades:***", value="**Reforge:**  Festive\n**Modifiers:**     Recombulator 3000\n**Gemstones:**   2x Perfect Aqua\n**Enchants:**  Bobbin' Time V\n**Attributes:** Fishing Experience 10", inline=True)
            embed4.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374149467124793364/magma_lord_boots.png")
            
            await interaction.response.send_message(embeds=[embed1, embed2, embed3, embed4], ephemeral=True)
        
        elif custom_id == "fishing_equipment":
            embed1 = discord.Embed(
                title="Thunderbolt Necklace",
                description=" ",
                color=discord.Color.orange()
            )
            embed1.add_field(name="***Upgrades:***", value="**Reforge:**  Snowy\n**Modifiers:**     Recombulator 3000", inline=True)
            embed1.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374777154675540139/Thunderbolt_Necklace.png")
            
            embed2 = discord.Embed(
                title="Gillsplash Cloak 10✪",
                description="",
                color=discord.Color.orange()
            )
            embed2.add_field(name="***Upgrades:***", value="**Reforge:**  Snowy\n**Modifiers:**     Recombulator 3000", inline=True)
            embed2.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374525958458970322/latest.png")
            
            embed3 = discord.Embed(
                title="Gillsplash Belt 10✪",
                description="",
                color=discord.Color.orange()
            )
            embed3.add_field(name="***Upgrades:***", value="**Reforge:**  Snowy\n**Modifiers:**     Recombulator 3000", inline=True)
            embed3.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374526000699801621/latest.png")
            
            embed4 = discord.Embed(
                title="Gillsplash Gloves 10✪",
                description="",
                color=discord.Color.orange()
            )
            embed4.add_field(name="***Upgrades:***", value="**Reforge:**  Snowy\n**Modifiers:**     Recombulator 3000", inline=True)
            embed4.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374526044274430033/latest.png")
            
            await interaction.response.send_message(embeds=[embed1, embed2, embed3, embed4], ephemeral=True)
        
        elif custom_id == "fishing_pet":
            embed = discord.Embed(
                title="Ammonite",
                description=" ",
                color=discord.Color.teal()
            )
            embed.add_field(name="***Upgrades:***", value="**Pet Item:**  Burnt Texts", inline=True)
            embed.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374149564159754311/a074a7bd976fe6aba1624161793be547d54c835cf422243a851ba09d1e650553.png")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif custom_id == "fishing_rod":
            embed = discord.Embed(
                title="Hellfire Rod 6✪",
                description=" ",
                color=discord.Color.dark_red()
            )
            embed.add_field(name="***Upgrades:***", value="**Reforge:**  Pitchin'\n**Modifiers:**     Recombulator 3000\n**Gemstones:**   2x Perfect Aqua\n**Enchants:**  Flash 5 / everything beside Corruption\n**Attributes:** Double Hook 10 / Fishing Speed 10\n**Parts:** Titan Line", inline=True)
            embed.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374149629637034074/hellfire_rod.png")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif custom_id == "fishing_weapons":
            embed1 = discord.Embed(
                title="Hyperion",
                description="Killer Weapon",
                color=discord.Color.purple()
            )
            embed1.add_field(name="***Upgrades:***", value="**Enchant:**  Looting 5", inline=True)
            embed1.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374150077974577306/hyperion.png")
            
            embed2 = discord.Embed(
                title="Dreadlord Sword",
                description="Loot Share Weapon",
                color=discord.Color.dark_grey()
            )
            embed2.add_field(name="***Upgrades:***", value="**Enchant:**  None", inline=True)
            embed2.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1374149764853268642/dreadlord_sword.png")
            
            await interaction.response.send_message(embeds=[embed1, embed2], ephemeral=True)

        elif custom_id == "power_stone":
            embed = discord.Embed(
                title="No Power",
                description=" ",
                color=discord.Color.dark_green()
            )
            embed.add_field(name="***Tuning:***", value="*None*", inline=True)
            embed.set_thumbnail(url="https://cdn.discordapp.com/attachments/1374148859034468496/1375837261480067152/Glass_JE4_BE2.webp?ex=683323cc&is=6831d24c&hm=db054d393fff2d050f209af73160adf683d7e6a84ab40ab1c6cc069dc20af3e5&")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)

        elif custom_id == "fishing_hotm":
            embed = discord.Embed(
                title="Heart of the Mountain (HOTM)",
                description=" ",
                color=discord.Color.dark_orange()
            )
            embed.add_field(name="***Specs:***", value="**HOTM:**  6\n**Perks:**    Subterranean Fisher / Quick Forge\n**Note:**  Higher HOTM = More Double Hook",inline=True)
            embed.set_image(url="https://cdn.discordapp.com/attachments/1373747930066063472/1374151457196085329/image.png")
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif custom_id == "cage_setup":
            embed = discord.Embed(
                title="Cage Setup",
                description="Proper cage placement for worm fishing:",
                color=discord.Color.greyple()
            )
            embed.set_image(url="https://cdn.discordapp.com/attachments/1373747930066063472/1374121091529969834/cageonline-video-cutter.com-ezgif.com-video-to-gif-converter.gif")
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif custom_id == "odin_mod":
            embed = discord.Embed(
                title="Odin",
                color=discord.Color.blue()
            )
            embed.set_image(url="https://cdn.discordapp.com/attachments/1373747930066063472/1373964133426270248/Recording2025-05-19113500online-video-cutter.com1-ezgif.com-video-to-gif-converter.gif")
            embed.add_field(
                name="Download", 
                value="[Get Odin Mod](https://github.com/odtheking/Odin/releases)", 
                inline=False
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif custom_id == "chattriggers_mod":
            embed = discord.Embed(
                title="Chattriggers",
                color=discord.Color.greyple()
            )
            embed.add_field(
                name="Download", 
                value="[Get Chattrriggers Mod](https://www.chattriggers.com/)", 
                inline=False
            )
            embed.add_field(
                name="Recommended Modules",
                value=(
                    "`/ct import FeeshNotifier`\n"
                    "This is a Worm profit tracker\n"
                    "Accessible with `/feesh`\n\n"
                    "`/ct import BoopInv`\n"
                    "When you get booped (`/boop [name]`) you send a party invite to that player"
                ),
                inline=False
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        
        elif custom_id == "neu_mod":
            embed = discord.Embed(
                title="NEU (Not Enough Updates)",
                color=discord.Color.green()
            )
            embed.add_field(
                name="Download", 
                value="[Get NEU Mod](https://github.com/NotEnoughUpdates/NotEnoughUpdates/releases)", 
                inline=False
            )
            embed.add_field(
                name="Important Command",
                value=(
                    "`/neurename`\n"
                    "This command allows you to rename a specific item so Taunahi recognizes it as the custom named item\n"
                    "Usage: Hold the item you want to rename and type `/neurename <new name>`"
                ),
                inline=False
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
    except Exception as e:
        logging.error(f"Error handling interaction: {e}")
        if not interaction.response.is_done():
            await interaction.response.send_message("An error occurred while processing your request.", ephemeral=True)

@bot.tree.command(
    name="ratestats",
    description="Show current rate limiter statistics",
    guild=GUILD_OBJECT
)
async def ratestats(interaction: discord.Interaction):
    if interaction.user.id != CONFIG["AUTHORIZED_USER_ID"]:
        await interaction.response.send_message(
            "You don't have permission to use this command.",
            ephemeral=True
        )
        return
        
    stats = rate_limiter.get_stats()
    
    embed = discord.Embed(
        title="Rate Limiter Statistics",
        color=discord.Color.blue()
    )
    
    embed.add_field(
        name="Current Usage",
        value=f"{stats['current_usage']}/{stats['max_requests']} requests in {stats['time_window']}s window",
        inline=False
    )
    
    embed.add_field(
        name="Total Requests",
        value=f"{stats['total_requests']:,}",
        inline=True
    )
    
    embed.add_field(
        name="Rate Limited Count",
        value=f"{stats['rate_limited_count']:,}",
        inline=True
    )
    
    embed.add_field(
        name="Requests per Second",
        value=f"{stats['requests_per_second']:.2f}",
        inline=True
    )
    
    uptime = timedelta(seconds=int(stats['uptime_seconds']))
    embed.add_field(
        name="Uptime",
        value=str(uptime),
        inline=True
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(
    name="ratereset",
    description="Reset rate limiter statistics",
    guild=GUILD_OBJECT
)
async def ratereset(interaction: discord.Interaction):
    if interaction.user.id != CONFIG["AUTHORIZED_USER_ID"]:
        await interaction.response.send_message(
            "You don't have permission to use this command.",
            ephemeral=True
        )
        return
        
    rate_limiter.reset_stats()
    await interaction.response.send_message(
        "Rate limiter statistics have been reset.",
        ephemeral=True
    )

@bot.tree.command(
    name="macroadd",
    description="Create a macro check embed with video",
    guild=GUILD_OBJECT
)
@app_commands.describe(
    video_url="URL to the video (Discord attachment link)",
    account_name="Name of the account being checked",
    custom_name="Custom name to display in header (can mention user with @)",
    check_type="Type of check being performed",
    ban_status="Whether the account was banned (Yes/No)",
    macro_duration="How long the macro was running"
)
async def macroadd(
    interaction: discord.Interaction,
    video_url: str,
    account_name: str,
    custom_name: str,
    check_type: str,
    ban_status: str,
    macro_duration: str
):
    try:
        channel = bot.get_channel(CONFIG["MACRO_CHECKS_CHANNEL_ID"])
        if channel is None:
            raise ValueError("Could not find the macro checks channel")
        
        # Create player head URL
        player_head_url = f"https://mc-heads.net/avatar/{account_name}/128"
        
        # Check if custom_name is a user mention
        if custom_name.startswith('<@') and custom_name.endswith('>'):
            display_name = custom_name
        else:
            try:
                user_id = int(custom_name)
                user = await bot.fetch_user(user_id)
                display_name = user.mention
            except (ValueError, discord.NotFound):
                display_name = custom_name
        
        embed = discord.Embed(
            title=f"Macro Check - {display_name}",
            color=discord.Color.red(),
            description=f"** **"
        )
        
        embed.add_field(name=f" ", value=f" ", inline=False)
        embed.add_field(
            name=f" ", 
            value=f"**Type of Check:** {check_type}\n**Macro Duration:** {macro_duration}\n**Ban:** {ban_status}", 
            inline=False
        )
        embed.set_thumbnail(url=player_head_url)
        embed.add_field(name=" ", value=f"[**Video**]({video_url})", inline=False)
        
        if interaction.user.id != CONFIG["AUTHORIZED_USER_ID"]:
            instruction_embed = discord.Embed(
                title="Macro Check",
                color=discord.Color.blue(),
                description=" "
            )
            instruction_embed.add_field(
                name="Instructions",
                value=(
                    f"Send {CONFIG['SUPPORT_CONTACT_MENTION']} ({CONFIG['SUPPORT_CONTACT']}) a video of the macro check, "
                    "the type of check you got, the macro duration and your ban status"
                ),
                inline=False
            )
            await interaction.response.send_message(embed=instruction_embed, ephemeral=True)
        else:
            await channel.send(embed=embed)
            await interaction.response.send_message("Macro check added successfully!", ephemeral=True)
        
    except Exception as e:
        error_msg = "❌ Failed to create macro embed"
        logging.error(f"{error_msg}: {e}")
        await interaction.response.send_message(error_msg, ephemeral=True)

@bot.tree.command(
    name="macrostats",
    description="Show macro check statistics",
    guild=GUILD_OBJECT
)
async def macrostats(interaction: discord.Interaction):
    try:
        channel = bot.get_channel(CONFIG["MACRO_CHECKS_CHANNEL_ID"])
        if channel is None:
            raise ValueError("Could not find the macro checks channel")
        
        count = 0
        async for _ in channel.history(limit=None):
            count += 1

        macro_channel_url = (
            f"https://discord.com/channels/{CONFIG['GUILD_ID']}/{CONFIG['MACRO_CHECKS_CHANNEL_ID']}"
        )

        embed = discord.Embed(
            title="Macro Check Statistics",
            color=discord.Color.blue(),
            description=" "
        )
        embed.add_field(
            name="Confirmed Macro checks",
            value=str(count),
            inline=False
        )
        embed.add_field(
            name="All Macro Checks",
            value=macro_channel_url,
            inline=False
        )
        embed.add_field(
            name="You got Macro checked?",
            value=(
                f"Send {CONFIG['SUPPORT_CONTACT_MENTION']} ({CONFIG['SUPPORT_CONTACT']}) a video of the macro check, "
                "the type of check you got, the macro duration and your ban status"
            ),
            inline=False
        )
        
        await interaction.response.send_message(embed=embed)
        
    except Exception as e:
        error_msg = "❌ Failed to create stats embed"
        logging.error(f"{error_msg}: {e}")
        await interaction.response.send_message(error_msg, ephemeral=True)

# Run the bot with rate limit handling
async def main():
    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        bot_token = get_bot_token()

        # Create aiohttp ClientSession
        async with aiohttp.ClientSession() as session:
            bot.http.session = session
            
            try:
                await bot.start(bot_token)
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limit error
                    logging.error(f"Rate limited on startup. Please wait a few minutes before trying again. Details: {e}")
                else:
                    logging.error(f"HTTP Error on startup: {e}")
                raise
            except Exception as e:
                logging.error(f"Failed to start bot: {e}")
                raise
            finally:
                if not bot.is_closed():
                    await bot.close()
    
    except Exception as e:
        logging.error(f"Critical error in main: {e}")
        raise
    finally:
        # Ensure we're cleaning up any remaining sessions
        await asyncio.sleep(1)  # Give time for cleanup
        for task in asyncio.all_tasks():
            if not task.done():
                task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot shutdown via keyboard interrupt")
    except Exception as e:
        logging.error(f"Bot crashed: {e}")
        raise
