import discord
from discord import app_commands
from discord.ext import commands, tasks
import asyncio
import random
import logging
import re
import os
import json
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Union

# -----------------------------
# Configuration
# -----------------------------
BOT_TOKEN = "YOUR_BOT_TOKEN"  # Keep your token secure!
BOT_PREFIX = "!" # Or None if only using slash commands
INTENTS = discord.Intents.default()
INTENTS.message_content = True # Needed for message history checks potentially
INTENTS.members = True # Needed to resolve user/role objects reliably
INTENTS.guilds = True # Needed for guild operations like getting members, roles, channels

# --- Logging ---
logger = logging.getLogger('discord.giveaway') # More specific logger name
logger.setLevel(logging.INFO)
log_directory = "logs"
os.makedirs(log_directory, exist_ok=True)
handler = logging.FileHandler(filename=os.path.join(log_directory,'giveaway_bot.log'), encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(handler)
# Add console logging too
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
logger.addHandler(stream_handler)

# --- Storage ---
STORAGE_DIR = "storage"
os.makedirs(STORAGE_DIR, exist_ok=True)
# Updated: Storage is now per-guild

ACTIVE_GIVEAWAYS_FILENAME = "active_giveaways.json"
ENDED_GIVEAWAYS_FILENAME = "ended_giveaways_temp.json" # For reroll cache
USER_STATS_FILENAME = "user_stats.json" # New file for user stats
GUILD_SETTINGS_FILENAME = "settings.json"

# --- Constants ---
GIVEAWAY_JOIN_ID = "gw_join_persistent"
GIVEAWAY_LIST_ID = "gw_list_persistent"
# Renamed/Modified End button ID
GIVEAWAY_END_BUTTON_ID = "gw_end_button_persistent"
# New Reroll button ID
GIVEAWAY_REROLL_BUTTON_ID = "gw_reroll_button_persistent"
# Link button is standard discord.ui.Button(style=discord.ButtonStyle.link)

MAX_ENDED_GIVEAWAYS_STORED = 50 # Limit how many ended GAs are kept for reroll per guild

# -------------------------------------------------------------------
# Guild Settings Data Class (Updated)
# -------------------------------------------------------------------
@dataclass
class GuildSettings:
    guild_id: int
    next_giveaway_id: int = 1 # Sequential ID counter for this guild
    staff_role_id: Optional[int] = None
    default_blacklist_role_id: Optional[int] = None
    default_bypass_role_ids: List[int] = field(default_factory=list)
    log_channel_id: Optional[int] = None

    # --- New Customizable Settings ---
    # Embed Appearance
    embed_colour: str = "#3498db" # Default blue
    embed_winners_colour: str = "#2ecc71" # Default green
    embed_nowinners_colour: str = "#e74c3c" # Default red
    embed_cancelled_colour: str = "#7f8c8d" # Default gray

    embed_description: str = "React with <:EventsHost:1368365113521995858> to enter!" # Use the new emoji placeholder
    embed_drop_description: str = "Be the first to click <:EventsHost:1368365113521995858> to win instantly!"

    embed_header: str = "🎉 **GIVEAWAY** 🎉"
    embed_header_end: str = "🎁 **GIVEAWAY ENDED** 🎁"
    embed_footer: str = "Giveaway ID: {giveaway_id}" # Remove message ID placeholder

    # Channel Messages
    win_message: str = "Congratulations {winners}! You won **{prize}**!"
    nowinners_message: str = "The giveaway for **{prize}** has ended, but there were no eligible participants."
    reroll_message: str = "🎉 **Reroll!** New winner(s) for **{prize}**: {winners}!"

    # DM Settings
    dm_winner: bool = True # Default to True
    title_dm_hostembed: str = "Giveaway Hosted"
    colour_dm_hostembed: str = "#3498db"
    description_dm_hostembed: str = "Your giveaway for **{prize}** in {guild_name} has started!"
    thumbnail_dm_hostembed: Optional[str] = None
    footer_dm_hostembed: str = "Giveaway ID: {giveaway_id}"

    title_dm_winembed: str = "You Won a Giveaway!"
    colour_dm_winembed: str = "#2ecc71"
    description_dm_winembed: str = "Congratulations! You won the giveaway for **{prize}** in {guild_name}!"
    thumbnail_dm_winembed: Optional[str] = None
    footer_dm_winembed: str = "Giveaway ID: {giveaway_id}"


    def to_dict(self) -> dict:
        return {
            "guild_id": self.guild_id,
            "next_giveaway_id": self.next_giveaway_id,
            "staff_role_id": self.staff_role_id,
            "default_blacklist_role_id": self.default_blacklist_role_id,
            "default_bypass_role_ids": self.default_bypass_role_ids,
            "log_channel_id": self.log_channel_id,
            # New fields
            "embed_colour": self.embed_colour,
            "embed_winners_colour": self.embed_winners_colour,
            "embed_nowinners_colour": self.embed_nowinners_colour,
            "embed_cancelled_colour": self.embed_cancelled_colour,
            "embed_description": self.embed_description,
            "embed_drop_description": self.embed_drop_description,
            "embed_header": self.embed_header,
            "embed_header_end": self.embed_header_end,
            "embed_footer": self.embed_footer,
            "win_message": self.win_message,
            "nowinners_message": self.nowinners_message,
            "reroll_message": self.reroll_message,
            "dm_winner": self.dm_winner,
            "title_dm_hostembed": self.title_dm_hostembed,
            "colour_dm_hostembed": self.colour_dm_hostembed,
            "description_dm_hostembed": self.description_dm_hostembed,
            "thumbnail_dm_hostembed": self.thumbnail_dm_hostembed,
            "footer_dm_hostembed": self.footer_dm_hostembed,
            "title_dm_winembed": self.title_dm_winembed,
            "colour_dm_winembed": self.colour_dm_winembed,
            "description_dm_winembed": self.description_dm_winembed,
            "thumbnail_dm_winembed": self.thumbnail_dm_winembed,
            "footer_dm_winembed": self.footer_dm_winembed,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'GuildSettings':
        return cls(
            guild_id=data["guild_id"],
            next_giveaway_id=data.get("next_giveaway_id", 1),
            staff_role_id=data.get("staff_role_id"),
            default_blacklist_role_id=data.get("default_blacklist_role_id"),
            default_bypass_role_ids=data.get("default_bypass_role_ids", []),
            log_channel_id=data.get("log_channel_id"),
            # New fields with defaults for backward compatibility
            embed_colour=data.get("embed_colour", "#3498db"),
            embed_winners_colour=data.get("embed_winners_colour", "#2ecc71"),
            embed_nowinners_colour=data.get("embed_nowinners_colour", "#e74c3c"),
            embed_cancelled_colour=data.get("embed_cancelled_colour", "#7f8c8d"),
            embed_description=data.get("embed_description", "React with <:EventsHost:1368365113521995858> to enter!"),
            embed_drop_description=data.get("embed_drop_description", "Be the first to click <:EventsHost:1368365113521995858> to win instantly!"),
            embed_header=data.get("embed_header", "🎉 **GIVEAWAY** 🎉"),
            embed_header_end=data.get("embed_header_end", "🎁 **GIVEAWAY ENDED** 🎁"),
            embed_footer=data.get("embed_footer", "Giveaway ID: {giveaway_id}"), # Update default footer
            win_message=data.get("win_message", "Congratulations {winners}! You won **{prize}**!"),
            nowinners_message=data.get("nowinners_message", "The giveaway for **{prize}** has ended, but there were no eligible participants."),
            reroll_message=data.get("reroll_message", "🎉 **Reroll!** New winner(s) for **{prize}**: {winners}!"),
            dm_winner=data.get("dm_winner", True),
            title_dm_hostembed=data.get("title_dm_hostembed", "Giveaway Hosted"),
            colour_dm_hostembed=data.get("colour_dm_hostembed", "#3498db"),
            description_dm_hostembed=data.get("description_dm_hostembed", "Your giveaway for **{prize}** in {guild_name} has started!"),
            thumbnail_dm_hostembed=data.get("thumbnail_dm_hostembed"),
            footer_dm_hostembed=data.get("footer_dm_hostembed", "Giveaway ID: {giveaway_id}"),
            title_dm_winembed=data.get("title_dm_winembed", "You Won a Giveaway!"),
            colour_dm_winembed=data.get("colour_dm_winembed", "#2ecc71"),
            description_dm_winembed=data.get("description_dm_winembed", "Congratulations! You won the giveaway for **{prize}** in {guild_name}!"),
            thumbnail_dm_winembed=data.get("thumbnail_dm_winembed"),
            footer_dm_winembed=data.get("footer_dm_winembed", "Giveaway ID: {giveaway_id}"),
        )


# -------------------------------------------------------------------
# Data Class for Giveaway State (Updated)
# -------------------------------------------------------------------
@dataclass
class GiveawayData:
    giveaway_id: int # Sequential ID for the guild
    message_id: int # The Discord message ID (still needed for interactions)
    channel_id: int # The channel the giveaway message is in
    guild_id: int
    prize: str
    host_id: int
    winners_count: int
    start_time: datetime
    end_time: datetime
    required_role_id: Optional[int] = None
    bonus_entries: Dict[int, int] = field(default_factory=dict) # role_id: bonus_count
    bypass_role_ids: List[int] = field(default_factory=list) # Bypass requirements
    blacklist_role_id: Optional[int] = None # Blacklisted roles cannot join (unless bypassed)
    min_messages: int = 0
    message_count_channel_id: Optional[int] = None # Channel to count messages in
    message_cooldown_seconds: int = 0 # Cooldown between counted messages per user
    required_keywords: List[str] = field(default_factory=list) # Keywords for counting messages
    donor_id: Optional[int] = None # User ID of the donor
    image_url: Optional[str] = None
    participants: Dict[int, int] = field(default_factory=dict) # user_id: entry_count
    ended: bool = False
    task_scheduled: bool = False # To track if end task is running
    is_drop: bool = False # NEW FIELD: To identify drop giveaways

    # Method to easily convert to dict for JSON storage
    def to_dict(self) -> dict:
        return {
            "giveaway_id": self.giveaway_id,
            "message_id": self.message_id,
            "channel_id": self.channel_id,
            "guild_id": self.guild_id,
            "prize": self.prize,
            "host_id": self.host_id,
            "winners_count": self.winners_count,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "required_role_id": self.required_role_id,
            "bonus_entries": {str(k): v for k, v in self.bonus_entries.items()},
            "bypass_role_ids": self.bypass_role_ids,
            "blacklist_role_id": self.blacklist_role_id,
            "min_messages": self.min_messages,
            "message_count_channel_id": self.message_count_channel_id,
            "message_cooldown_seconds": self.message_cooldown_seconds,
            "required_keywords": self.required_keywords,
            "donor_id": self.donor_id,
            "image_url": self.image_url,
            "participants": {str(k): v for k, v in self.participants.items()},
            "ended": self.ended,
            "is_drop": self.is_drop, # Save new field
        }

    # Class method to easily create from dict (loaded from JSON)
    @classmethod
    def from_dict(cls, data: dict) -> 'GiveawayData':
        start_time = datetime.fromisoformat(data["start_time"])
        end_time = datetime.fromisoformat(data["end_time"])
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        return cls(
            giveaway_id=data.get("giveaway_id", data.get("message_id", 0)), # Handle old data or default
            message_id=data["message_id"],
            channel_id=data["channel_id"],
            guild_id=data["guild_id"],
            prize=data["prize"],
            host_id=data["host_id"],
            winners_count=data["winners_count"],
            start_time=start_time,
            end_time=end_time,
            required_role_id=data.get("required_role_id"),
            bonus_entries={int(k): v for k in data.get("bonus_entries", {}).keys() for v in [data["bonus_entries"][k]]}, # Ensure correct type conversion
            bypass_role_ids=data.get("bypass_role_ids", []),
            blacklist_role_id=data.get("blacklist_role_id"),
            min_messages=data.get("min_messages", 0),
            message_count_channel_id=data.get("message_count_channel_id"),
            message_cooldown_seconds=data.get("message_cooldown_seconds", 0),
            required_keywords=data.get("required_keywords", []),
            donor_id=data.get("donor_id"),
            image_url=data.get("image_url"),
            participants={int(k): v for k in data.get("participants", {}).keys() for v in [data["participants"][k]]}, # Ensure correct type conversion
            ended=data.get("ended", False),
            is_drop=data.get("is_drop", False), # Load new field with default
        )

# -------------------------------------------------------------------
# User Statistics Data Class (New)
# -------------------------------------------------------------------
@dataclass
class UserGiveawayStats:
    user_id: int
    guild_id: int
    hosted_count: int = 0
    hosted_last_timestamp: Optional[datetime] = None
    donated_count: int = 0
    donated_last_timestamp: Optional[datetime] = None
    won_count: int = 0
    won_last_timestamp: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "guild_id": self.guild_id,
            "hosted_count": self.hosted_count,
            "hosted_last_timestamp": self.hosted_last_timestamp.isoformat() if self.hosted_last_timestamp else None,
            "donated_count": self.donated_count,
            "donated_last_timestamp": self.donated_last_timestamp.isoformat() if self.donated_last_timestamp else None,
            "won_count": self.won_count,
            "won_last_timestamp": self.won_last_timestamp.isoformat() if self.won_last_timestamp else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'UserGiveawayStats':
        hosted_last_timestamp = datetime.fromisoformat(data["hosted_last_timestamp"]) if data.get("hosted_last_timestamp") else None
        if hosted_last_timestamp and hosted_last_timestamp.tzinfo is None: hosted_last_timestamp = hosted_last_timestamp.replace(tzinfo=timezone.utc)

        donated_last_timestamp = datetime.fromisoformat(data["donated_last_timestamp"]) if data.get("donated_last_timestamp") else None
        if donated_last_timestamp and donated_last_timestamp.tzinfo is None: donated_last_timestamp = donated_last_timestamp.replace(tzinfo=timezone.utc)

        won_last_timestamp = datetime.fromisoformat(data["won_last_timestamp"]) if data.get("won_last_timestamp") else None
        if won_last_timestamp and won_last_timestamp.tzinfo is None: won_last_timestamp = won_last_timestamp.replace(tzinfo=timezone.utc)

        return cls(
            user_id=data["user_id"],
            guild_id=data["guild_id"],
            hosted_count=data.get("hosted_count", 0),
            hosted_last_timestamp=hosted_last_timestamp,
            donated_count=data.get("donated_count", 0),
            donated_last_timestamp=donated_last_timestamp,
            won_count=data.get("won_count", 0),
            won_last_timestamp=won_last_timestamp,
        )


# -------------------------------------------------------------------
# Storage Management Functions (Updated for per-guild and user stats)
# -------------------------------------------------------------------
def get_guild_dir(guild_id: int) -> str:
    """Gets the storage directory path for a specific guild."""
    guild_dir = os.path.join(STORAGE_DIR, str(guild_id))
    os.makedirs(guild_dir, exist_ok=True)
    return guild_dir

def get_guild_giveaways_file(guild_id: int, is_ended: bool = False) -> str:
    """Gets the file path for active or ended giveaways for a guild."""
    filename = ENDED_GIVEAWAYS_FILENAME if is_ended else ACTIVE_GIVEAWAYS_FILENAME
    return os.path.join(get_guild_dir(guild_id), filename)

def get_guild_settings_file(guild_id: int) -> str:
     """Gets the file path for guild settings."""
     return os.path.join(get_guild_dir(guild_id), GUILD_SETTINGS_FILENAME)

def save_guild_settings(settings: GuildSettings):
    """Saves guild settings to its file."""
    try:
        file_path = get_guild_settings_file(settings.guild_id)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(settings.to_dict(), f, indent=4)
        logger.debug(f"Saved settings for guild {settings.guild_id}")
    except Exception as e:
        logger.error(f"Failed to save settings for guild {settings.guild_id}: {e}", exc_info=True)

def load_guild_settings(guild_id: int) -> GuildSettings:
    """Loads guild settings from its file, or returns default if not found."""
    file_path = get_guild_settings_file(guild_id)
    if not os.path.exists(file_path):
        logger.info(f"No settings file found for guild {guild_id}. Returning default.")
        return GuildSettings(guild_id=guild_id) # Return default settings

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            settings = GuildSettings.from_dict(data)
            logger.debug(f"Loaded settings for guild {guild_id}")
            return settings
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from settings file for guild {guild_id}. File might be corrupt or empty. Returning default settings.", exc_info=True)
        return GuildSettings(guild_id=guild_id)
    except Exception as e:
        logger.error(f"Failed to load settings for guild {guild_id}: {e}", exc_info=True)
        return GuildSettings(guild_id=guild_id)


def save_giveaways_for_guild(giveaways: Dict[int, GiveawayData], guild_id: int, is_ended: bool = False):
    """Saves active or ended giveaways for a specific guild."""
    try:
        file_path = get_guild_giveaways_file(guild_id, is_ended)
        # Filter out ended giveaways if saving active ones, or vice-versa
        filtered_giveaways = {
            str(msg_id): gw.to_dict() for msg_id, gw in giveaways.items()
            if gw.ended == is_ended # Only save if ended status matches
        }
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_giveaways, f, indent=4)
        logger.debug(f"Saved {len(filtered_giveaways)} {'ended' if is_ended else 'active'} giveaways for guild {guild_id}")
    except Exception as e:
        logger.error(f"Failed to save giveaways for guild {guild_id}: {e}", exc_info=True)

def load_giveaways_for_guild(guild_id: int, is_ended: bool = False) -> Dict[int, GiveawayData]:
    """Loads active or ended giveaways for a specific guild."""
    giveaways = {}
    file_path = get_guild_giveaways_file(guild_id, is_ended)
    if not os.path.exists(file_path):
        return giveaways

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            giveaway_dicts = json.load(f)
            for msg_id_str, gw_dict in giveaway_dicts.items():
                try:
                    msg_id = int(msg_id_str)
                    giveaways[msg_id] = GiveawayData.from_dict(gw_dict)
                except Exception as e:
                    logger.error(f"Failed to load individual giveaway {msg_id_str} for guild {guild_id}: {e}")
            logger.debug(f"Loaded {len(giveaways)} {'ended' if is_ended else 'active'} giveaways from {file_path}")
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from {'ended' if is_ended else 'active'} giveaways file for guild {guild_id}. File might be corrupt or empty.", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to load giveaways for guild {guild_id}: {e}", exc_info=True)
    return giveaways

# --- Storage for User Stats (New) ---
def get_guild_user_stats_file(guild_id: int) -> str:
    """Gets the file path for user stats for a guild."""
    return os.path.join(get_guild_dir(guild_id), USER_STATS_FILENAME)

def load_guild_user_stats(guild_id: int) -> Dict[int, UserGiveawayStats]:
    """Loads user stats for a guild."""
    stats = {}
    file_path = get_guild_user_stats_file(guild_id)
    if not os.path.exists(file_path):
        return stats

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            stats_dict = json.load(f)
            for user_id_str, user_stats_dict in stats_dict.items():
                try:
                    user_id = int(user_id_str)
                    stats[user_id] = UserGiveawayStats.from_dict(user_stats_dict)
                except Exception as e:
                    logger.error(f"Failed to load individual user stats {user_id_str} for guild {guild_id}: {e}")
            logger.debug(f"Loaded {len(stats)} user stats from {file_path} for guild {guild_id}")
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from user stats file for guild {guild_id}. File might be corrupt or empty.", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to load user stats for guild {guild_id}: {e}", exc_info=True)
    return stats

def save_guild_user_stats(stats: Dict[int, UserGiveawayStats], guild_id: int):
    """Saves user stats for a guild."""
    try:
        file_path = get_guild_user_stats_file(guild_id)
        stats_to_save = {str(user_id): user_stats.to_dict() for user_id, user_stats in stats.items()}
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(stats_to_save, f, indent=4)
        logger.debug(f"Saved {len(stats)} user stats for guild {guild_id}")
    except Exception as e:
        logger.error(f"Failed to save user stats for guild {guild_id}: {e}", exc_info=True)


# -------------------------------------------------------------------
# Duration Parser (Slightly improved for clarity)
# -------------------------------------------------------------------
def parse_duration(duration_str: str) -> Optional[timedelta]:
    """
    Converts a string like '1h30m' or '2d' or '45s' into a timedelta object.
    Returns None if the format is invalid or duration is non-positive.
    """
    if not duration_str:
        return None
    pattern = re.compile(r"(\d+)\s*(d|h|m|s)", re.IGNORECASE)
    matches = pattern.findall(duration_str)
    if not matches:
        return None

    total_seconds = 0
    for value, unit in matches:
        try:
            value = int(value)
            unit = unit.lower()
            if unit == 's':
                total_seconds += value
            elif unit == 'm':
                total_seconds += value * 60
            elif unit == 'h':
                total_seconds += value * 3600
            elif unit == 'd':
                total_seconds += value * 86400
            else: # Should not happen with current pattern but safety
                 return None
        except ValueError:
            return None # Invalid value

    if total_seconds <= 0:
        return None

    return timedelta(seconds=total_seconds)

# -------------------------------------------------------------------
# Giveaway Embed Generator (Updated)
# -------------------------------------------------------------------
def create_giveaway_embed(giveaway: GiveawayData, bot: commands.Bot, status: str = "active", guild_settings: Optional[GuildSettings] = None) -> discord.Embed:
    """Creates the Discord embed for a giveaway based on its data and status, using guild settings."""
    guild = bot.get_guild(giveaway.guild_id)
    host = guild.get_member(giveaway.host_id) if guild else bot.get_user(giveaway.host_id) or f"ID: {giveaway.host_id}"
    donor = guild.get_member(giveaway.donor_id) if guild and giveaway.donor_id else bot.get_user(giveaway.donor_id) if giveaway.donor_id else None

    # Use guild settings for customizable text and colors
    # Access settings via bot.giveaway_cog if not provided directly
    settings = guild_settings
    if not settings and hasattr(bot, 'giveaway_cog') and bot.giveaway_cog:
        settings = bot.giveaway_cog.guild_settings.get(giveaway.guild_id)
    if not settings: # Fallback to default settings if not found via either method
        settings = GuildSettings(guild_id=giveaway.guild_id)


    # Determine description based on giveaway type
    description_template = settings.embed_drop_description if giveaway.is_drop else settings.embed_description
    try: # Handle potential formatting errors
        description = description_template.format(prize=giveaway.prize, winners=giveaway.winners_count, host=host.mention if isinstance(host, (discord.User, discord.Member)) else str(host))
    except KeyError as e:
        logger.error(f"Formatting error in embed description for guild {giveaway.guild_id}. Missing key: {e}. Template: '{description_template}'", exc_info=True)
        description = description_template # Use template as fallback

    color = discord.Color.blue() # Default
    title = settings.embed_header # Default title
    end_time_str = ""

    if status == "active":
        try:
            color = discord.Color.from_rgb(*tuple(int(settings.embed_colour[i:i+2], 16) for i in (1, 3, 5)))
        except:
            logger.warning(f"Invalid embed_colour hex in settings for guild {giveaway.guild_id}: {settings.embed_colour}. Using default blue.")
            color = discord.Color.blue()
        title = settings.embed_header
        end_time_str = f"Ends: <t:{int(giveaway.end_time.timestamp())}:R> (<t:{int(giveaway.end_time.timestamp())}:F>)"
    elif status == "ended":
        try:
            color = discord.Color.from_rgb(*tuple(int(settings.embed_winners_colour[i:i+2], 16) for i in (1, 3, 5))) # Use winner color on end by default
        except:
             logger.warning(f"Invalid embed_winners_colour hex in settings for guild {giveaway.guild_id}: {settings.embed_winners_colour}. Using default gold.")
             color = discord.Color.gold()
        title = settings.embed_header_end
        end_time_str = f"Ended: <t:{int(giveaway.end_time.timestamp())}:F>"
    elif status == "ended_no_winners": # A specific status for clarity when ending with no winners
         try:
            color = discord.Color.from_rgb(*tuple(int(settings.embed_nowinners_colour[i:i+2], 16) for i in (1, 3, 5)))
         except:
             logger.warning(f"Invalid embed_nowinners_colour hex in settings for guild {giveaway.guild_id}: {settings.embed_nowinners_colour}. Using default red.")
             color = discord.Color.red()
         title = settings.embed_header_end
         end_time_str = f"Ended: <t:{int(giveaway.end_time.timestamp())}:F>"
    elif status == "cancelled":
        try:
            color = discord.Color.from_rgb(*tuple(int(settings.embed_cancelled_colour[i:i+2], 16) for i in (1, 3, 5)))
        except:
            logger.warning(f"Invalid embed_cancelled_colour hex in settings for guild {giveaway.guild_id}: {settings.embed_cancelled_colour}. Using default gray.")
            color = discord.Color.dark_gray()
        title = settings.embed_header_end # Or a specific cancelled header? Using end header for now.
        end_time_str = f"Cancelled: <t:{int(datetime.now(timezone.utc).timestamp())}:F>"
    else: # Default/Unknown status
        color = discord.Color.orange()
        title = "🎁 Giveaway 🎁"
        end_time_str = f"Ends: <t:{int(giveaway.end_time.timestamp())}:F>" # Fallback

    try: # Handle potential formatting errors in title
         formatted_title = title.format(prize=giveaway.prize, winners=giveaway.winners_count)
    except KeyError as e:
         logger.error(f"Formatting error in embed title for guild {giveaway.guild_id}. Missing key: {e}. Template: '{title}'", exc_info=True)
         formatted_title = title # Use template as fallback


    embed = discord.Embed(
        title=formatted_title, # Format title
        description=description,
        color=color,
        timestamp=giveaway.start_time if status == "active" else datetime.now(timezone.utc) # Use start time for active, now for ended/cancelled
    )

    # Add donor info below description but before fields
    if donor:
        embed.description += f"\nDonated by: {donor.mention if isinstance(donor, (discord.User, discord.Member)) else f'ID: {giveaway.donor_id}'}"

    embed.add_field(name="Winners", value=str(giveaway.winners_count), inline=True)
    if status in ["active", "ended", "ended_no_winners", "cancelled"]:
        embed.add_field(name="Time", value=end_time_str, inline=False) # Change 'Ends/Ended' to 'Time' for clarity


    # Add requirements/bonus info if applicable and active
    if status == "active" and not giveaway.is_drop: # Requirements/bonus only for normal giveaways
        requirements = []
        if giveaway.required_role_id:
            role = guild.get_role(giveaway.required_role_id) if guild else None
            requirements.append(f"- Must have role: {role.mention if role else f'ID: {giveaway.required_role_id}'}")

        if giveaway.min_messages > 0:
            count_channel = guild.get_channel(giveaway.message_count_channel_id) if guild and giveaway.message_count_channel_id else guild.get_channel(giveaway.channel_id) if guild else None
            channel_mention = count_channel.mention if count_channel else f"ID: {giveaway.message_count_channel_id or giveaway.channel_id}"
            req_text = f"- At least {giveaway.min_messages} messages sent in {channel_mention} since giveaway start"
            if giveaway.message_cooldown_seconds > 0:
                 req_text += f" (with >{giveaway.message_cooldown_seconds}s cooldown)"
            if giveaway.required_keywords:
                 req_text += f" (containing keywords: {', '.join(giveaway.required_keywords)})"
            requirements.append(req_text)

        # Add requirements field if any exist
        if requirements:
            embed.add_field(name="Requirements", value="\n".join(requirements), inline=False)


        # Display Bonus Entries
        if giveaway.bonus_entries:
            bonus_str = ""
            if guild:
                bonus_str = "\n".join(
                    f"{guild.get_role(rid).mention if guild.get_role(rid) else f'ID: {rid}'}: +{extra} entries"
                    for rid, extra in giveaway.bonus_entries.items()
                )
            else:
                 bonus_str = "\n".join(f"RoleID {rid}: +{extra} entries" for rid, extra in giveaway.bonus_entries.items())
            if bonus_str: embed.add_field(name="Bonus Entries", value=bonus_str, inline=False)


        # Display Bypass Roles
        all_bypass_roles = set(giveaway.bypass_role_ids)
        guild_settings_from_cog = bot.giveaway_cog.guild_settings.get(giveaway.guild_id) # Access settings from cog
        if guild_settings_from_cog and guild_settings_from_cog.default_bypass_role_ids:
             all_bypass_roles.update(guild_settings_from_cog.default_bypass_role_ids)

        if all_bypass_roles:
             bypass_str = ""
             if guild:
                 bypass_str = ", ".join(guild.get_role(rid).mention if guild.get_role(rid) else f'ID: {rid}' for rid in all_bypass_roles)
             else:
                 bypass_str = ", ".join(f"RoleID {rid}" for rid in all_bypass_roles)
             if bypass_str: embed.add_field(name="Bypasses Requirements", value=bypass_str, inline=False)

        # Display Blacklist Role
        all_blacklist_roles = set()
        if giveaway.blacklist_role_id:
            all_blacklist_roles.add(giveaway.blacklist_role_id)
        if guild_settings_from_cog and guild_settings_from_cog.default_blacklist_role_id:
             all_blacklist_roles.add(guild_settings_from_cog.default_blacklist_role_id)

        if all_blacklist_roles:
             blacklist_str = ""
             if guild:
                 blacklist_str = ", ".join(guild.get_role(rid).mention if guild.get_role(rid) else f'ID: {rid}' for rid in all_blacklist_roles)
             else:
                 blacklist_str = ", ".join(f"RoleID {rid}" for rid in all_blacklist_roles)
             if blacklist_str: embed.add_field(name="Cannot Join (Unless Bypassed)", value=blacklist_str, inline=False)


    if giveaway.image_url:
        embed.set_image(url=giveaway.image_url)

    # Use customizable footer text
    try: # Handle potential formatting errors in footer
         formatted_footer = settings.embed_footer.format(giveaway_id=giveaway.giveaway_id)
    except KeyError as e:
        logger.error(f"Formatting error in embed footer for guild {giveaway.guild_id}. Missing key: {e}. Template: '{settings.embed_footer}'", exc_info=True)
        formatted_footer = settings.embed_footer # Use template as fallback

    embed.set_footer(text=formatted_footer)
    # embed.timestamp is already handled above

    return embed


# -------------------------------------------------------------------
# Active Giveaway View (Used while giveaway is running) - NEW CLASS
# -------------------------------------------------------------------
class ActiveGiveawayView(discord.ui.View):
    def __init__(self, cog_ref):
        super().__init__(timeout=None) # Persistent View
        self.cog = cog_ref # Reference to the Giveaway cog instance

    # Add the buttons in __init__ for clarity of active state
    def __init__(self, cog_ref):
        super().__init__(timeout=None)
        self.cog = cog_ref
        # Add buttons directly
        self.add_item(discord.ui.Button(label="Join", style=discord.ButtonStyle.green, emoji="<:EventsHost:1368365113521995858>", custom_id=GIVEAWAY_JOIN_ID))
        # Initial label will be set by update_participant_count on load/start
        self.add_item(discord.ui.Button(label="0", style=discord.ButtonStyle.blurple, emoji="<:group:1369320729404899349>", custom_id=GIVEAWAY_LIST_ID)) # Participant count only
        self.add_item(discord.ui.Button(label="End", style=discord.ButtonStyle.red, custom_id=GIVEAWAY_END_BUTTON_ID))


    async def update_participant_count(self, interaction: discord.Interaction, giveaway: GiveawayData):
        """Helper to update the participants button label."""
        participant_count = len(giveaway.participants)
        for item in self.children:
            # Update label and emoji for the participants button
            if isinstance(item, discord.ui.Button) and item.custom_id == GIVEAWAY_LIST_ID:
                item.label = f"{participant_count}" # Only count
                # Emoji <:group:1369320729404899349> is already set in __init__
                break
        # The view needs to be updated on the message for the change to be visible.
        # This update is triggered by the interaction response or followup.
        # For participant count updates specifically, we might need to fetch the message
        # and edit the view *after* the initial interaction response, which is complex.
        # Let's rely on the join/leave interactions themselves to trigger a view update
        # via the followup.edit_message or interaction.edit_original_response.


    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, emoji="<:EventsHost:1368365113521995858>", custom_id=GIVEAWAY_JOIN_ID)
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        user = interaction.user
        guild = interaction.guild
        if user.bot:
            # Use followup after defer
            await interaction.response.defer(ephemeral=True)
            return await interaction.followup.send("Bots cannot join giveaways.", ephemeral=True)

        if not guild:
            # Use followup after defer
            await interaction.response.defer(ephemeral=True)
            return await interaction.followup.send("This command can only be used in a server.", ephemeral=True)

        # Defer response first, thinking=True is good for longer checks
        await interaction.response.defer(ephemeral=True, thinking=True)

        giveaway = self.cog.active_giveaways.get(interaction.message.id)
        if not giveaway or giveaway.ended or giveaway.guild_id != guild.id:
            return await interaction.followup.send("This giveaway is not active or has ended.", ephemeral=True)

        # --- Double-Click to Leave Logic ---
        if user.id in giveaway.participants:
            del giveaway.participants[user.id]
            self.cog.save_active_giveaways_for_guild(giveaway.guild_id)
            # Update participant count and view
            await self.update_participant_count(interaction, giveaway)
            await interaction.followup.send("You have left the giveaway.", ephemeral=True)
            logger.info(f"{user} (ID: {user.id}) left giveaway {giveaway.giveaway_id}/{giveaway.message.id} in guild {guild.id} by clicking Join again.")
            # Do NOT log leave event here as per new logging requirement (only start/end/cancel/reroll)
            return # User successfully left


        # --- If not leaving, proceed with Join Requirements Check ---
        member = guild.get_member(user.id) # Fetch fresh member object for roles
        if not member:
             return await interaction.followup.send("Could not verify your membership status.", ephemeral=True)

        # Combine giveaway-specific and default bypass roles
        all_bypass_roles = set(giveaway.bypass_role_ids)
        guild_settings = self.cog.guild_settings.get(guild.id)
        if guild_settings and guild_settings.default_bypass_role_ids:
             all_bypass_roles.update(guild_settings.default_bypass_role_ids)

        member_roles_set = {role.id for role in member.roles}
        has_bypass = any(role_id in all_bypass_roles for role_id in member_roles_set)


        # 1. Blacklist Role Check (if not bypassed)
        if not has_bypass:
             all_blacklist_roles = set()
             if giveaway.blacklist_role_id:
                 all_blacklist_roles.add(giveaway.blacklist_role_id)
             if guild_settings and guild_settings.default_blacklist_role_id:
                  all_blacklist_roles.add(guild_settings.default_blacklist_role_id)

             if any(role_id in all_blacklist_roles for role_id in member_roles_set):
                  blacklist_role_names = [guild.get_role(rid).name for rid in all_blacklist_roles if guild.get_role(rid)]
                  role_list_str = ", ".join(blacklist_role_names) if blacklist_role_names else "a blacklisted role"
                  return await interaction.followup.send(
                       f"You have {role_list_str} and cannot join this giveaway.", ephemeral=True
                  )


        # 2. Required Role Check (if not bypassed) - Only for normal giveaways
        if not has_bypass and not giveaway.is_drop and giveaway.required_role_id: # Add is_drop check
            required_role = guild.get_role(giveaway.required_role_id)
            if required_role and required_role.id not in member_roles_set:
                return await interaction.followup.send(
                    f"You need the **{required_role.name}** role to join this giveaway.", ephemeral=True
                )
            elif not required_role:
                 logger.warning(f"Required role ID {giveaway.required_role_id} not found in guild {guild.id} for giveaway {giveaway.message.id}")
                 return await interaction.followup.send("The required role for this giveaway seems to be missing. Please contact the host.", ephemeral=True)


        # 3. Minimum Message Check (if not bypassed) - Only for normal giveaways
        if not has_bypass and not giveaway.is_drop and giveaway.min_messages > 0: # Add is_drop check
            count_channel_id = giveaway.message_count_channel_id or giveaway.channel_id
            count_channel = guild.get_channel(count_channel_id)
            if not count_channel:
                logger.error(f"Giveaway message count channel {count_channel_id} not found for giveaway {giveaway.message.id}.")
                return await interaction.followup.send("Could not find the required message counting channel.", ephemeral=True)

            bot_member = guild.get_member(self.cog.bot.user.id)
            if not bot_member or not count_channel.permissions_for(bot_member).read_message_history:
                 logger.error(f"Bot lacks permission to read history in channel {count_channel.id} for giveaway {giveaway.message.id}")
                 return await interaction.followup.send(f"I don't have permission to check message history in {count_channel.mention}.", ephemeral=True)

            message_count = 0
            last_counted_message_time: Optional[datetime] = None

            try:
                async for msg in count_channel.history(limit=None, after=giveaway.start_time.replace(tzinfo=None), before=datetime.now(timezone.utc).replace(tzinfo=None)): # Remove tzinfo for comparison if needed
                     if msg.author.id == user.id:
                         if giveaway.required_keywords:
                             message_content_lower = msg.content.lower()
                             if not any(keyword.lower() in message_content_lower for keyword in giveaway.required_keywords):
                                  continue

                         if giveaway.message_cooldown_seconds > 0:
                              if last_counted_message_time is not None:
                                   time_since_last_counted = msg.created_at.replace(tzinfo=None) - last_counted_message_time.replace(tzinfo=None) # Compare naive datetimes
                                   if time_since_last_counted.total_seconds() < giveaway.message_cooldown_seconds:
                                        continue

                         message_count += 1
                         last_counted_message_time = msg.created_at # Keep original datetime with tzinfo


                if message_count < giveaway.min_messages:
                     channel_mention = count_channel.mention
                     req_text = f"You need to send at least {giveaway.min_messages} messages in {channel_mention}"
                     req_text += f" since the giveaway started (<t:{int(giveaway.start_time.timestamp())}:R>)."
                     if giveaway.message_cooldown_seconds > 0:
                         req_text += f" Messages must be sent with more than {giveaway.message_cooldown_seconds} seconds apart."
                     if giveaway.required_keywords:
                         req_text += f" Messages must contain one of the required keywords: {', '.join(giveaway.required_keywords)}."

                     req_text += f" You currently have {message_count} eligible message(s)."

                     return await interaction.followup.send(req_text, ephemeral=True)

            except discord.Forbidden:
                logger.error(f"Bot lacks permission to read history in channel {count_channel.id} for giveaway {giveaway.message.id}")
                return await interaction.followup.send(f"I don't have permission to check message history in {count_channel.mention}.", ephemeral=True)
            except Exception as e:
                logger.error(f"Error checking message count for {user} in giveaway {giveaway.message.id}: {e}", exc_info=True)
                return await interaction.followup.send("An error occurred while checking your message count. Please try again.", ephemeral=True)


        # --- Calculate Entries ---
        # If we reached here, requirements are met (or bypassed) and user is joining
        total_entries = 1 # Base entry
        if not giveaway.is_drop: # Bonus entries only for normal giveaways
            for role_id, bonus in giveaway.bonus_entries.items():
                if role_id in member_roles_set:
                    total_entries += bonus

        # --- Add Participant ---
        # Check again if it's a drop and already has participants (should only allow 1)
        if giveaway.is_drop and giveaway.participants:
             # This case handles if multiple people click *simultaneously* before the end logic runs
             # Only the absolute first one should win. If participants already exist, they were faster.
             return await interaction.followup.send("Someone else was faster!", ephemeral=True)

        giveaway.participants[user.id] = total_entries
        self.cog.save_active_giveaways_for_guild(giveaway.guild_id)

        # Update button label and view
        await self.update_participant_count(interaction, giveaway)
        # Edit the original message to update the view with the new participant count label
        try:
             await interaction.edit_original_response(view=self)
        except Exception as e:
             logger.warning(f"Failed to edit original response to update view for giveaway {giveaway.message.id}: {e}")


        # --- Handle Drop Giveaway Instant Win ---
        if giveaway.is_drop:
             # The first person to successfully join wins!
             # Double check if they are indeed the only participant (or first registered)
             if len(giveaway.participants) == 1 and list(giveaway.participants.keys())[0] == user.id:
                 await interaction.followup.send(f"You were the first to join the drop and won **{giveaway.prize}**!", ephemeral=True)
                 logger.info(f"{user} (ID: {user.id}) won drop giveaway {giveaway.giveaway_id}/{giveaway.message.id} instantly.")
                 # Immediately end the drop giveaway
                 await self.cog.end_giveaway(giveaway.message.id, ended_by=user, instant_winner=user.id) # Pass the winner ID
                 return # Stop further processing for drops
             else:
                 # If they joined but were not the first (race condition)
                 del giveaway.participants[user.id] # Remove their entry
                 self.cog.save_active_giveaways_for_guild(giveaway.guild_id) # Save the state
                 await self.update_participant_count(interaction, giveaway) # Update count display
                 try:
                    await interaction.edit_original_response(view=self)
                 except:
                    pass
                 return await interaction.followup.send("Someone else claimed the drop just before you!", ephemeral=True)


        # --- Normal Giveaway Join Success ---
        await interaction.followup.send(f"You have successfully joined the giveaway for **{giveaway.prize}** with **{total_entries}** entries!", ephemeral=True)
        logger.info(f"{user} (ID: {user.id}) joined giveaway {giveaway.giveaway_id}/{giveaway.message.id} in guild {guild.id} with {total_entries} entries.")
        # Do NOT log join event here as per new logging requirement


    @discord.ui.button(label="0", style=discord.ButtonStyle.blurple, emoji="<:group:1369320729404899349>", custom_id=GIVEAWAY_LIST_ID)
    async def participants_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Defer response
        await interaction.response.defer(ephemeral=True)

        giveaway = self.cog.active_giveaways.get(interaction.message.id)
        if not giveaway or giveaway.ended or giveaway.guild_id != guild.id:
            return await interaction.followup.send("This giveaway is not active.", ephemeral=True)

        if not giveaway.participants:
            return await interaction.followup.send("No one has joined the giveaway yet.", ephemeral=True)

        # Create a list of participants - fetch members for mentions
        participant_mentions = []
        for user_id, entries in giveaway.participants.items():
            member = guild.get_member(user_id)
            mention = member.mention if member else f"User ID: {user_id}"
            # Only show entries for non-drop giveaways
            entry_text = f" ({entries} entries)" if not giveaway.is_drop else ""
            participant_mentions.append(f"{mention}{entry_text}")


        description = "**Participants:**\n" + "\n".join(participant_mentions)
        if len(description) > 1900: # Keep buffer for safety
            description = description[:1900] + "\n... (list truncated)"

        await interaction.followup.send(description, ephemeral=True)


    @discord.ui.button(label="End", style=discord.ButtonStyle.red, custom_id=GIVEAWAY_END_BUTTON_ID)
    async def end_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        await interaction.response.defer(ephemeral=True, thinking=True)
        giveaway = self.cog.active_giveaways.get(interaction.message.id)

        if not giveaway or giveaway.ended or giveaway.guild_id != guild.id:
            return await interaction.followup.send("This giveaway is not active or already ended.", ephemeral=True)

        # Permission Check: Host or Staff Role or Manage Messages
        host_id = giveaway.host_id
        member = guild.get_member(interaction.user.id)
        if not member:
            return await interaction.followup.send("Could not verify your identity.", ephemeral=True)

        guild_settings = self.cog.guild_settings.get(guild.id)
        is_staff = False
        if guild_settings and guild_settings.staff_role_id:
            staff_role = guild.get_role(guild_settings.staff_role_id)
            if staff_role and staff_role in member.roles:
                 is_staff = True

        # Check if user is host OR has manage_messages OR is staff
        if interaction.user.id != host_id and not member.guild_permissions.manage_messages and not is_staff:
            perm_msg = "Only the host"
            can_list = []
            if is_staff: can_list.append("staff members")
            if member.guild_permissions.manage_messages: can_list.append("users with 'Manage Messages'")
            if can_list: perm_msg += " or " + " or ".join(can_list)
            perm_msg += " permission can end the giveaway."
            return await interaction.followup.send(perm_msg, ephemeral=True)


        # Cancel the scheduled end task (if it exists and is running)
        if self.cog.giveaway_end_tasks.get(interaction.message.id):
             self.cog.giveaway_end_tasks[interaction.message.id].cancel()
             del self.cog.giveaway_end_tasks[interaction.message.id]
             logger.info(f"Cancelled scheduled end task for giveaway {giveaway.giveaway_id}/{giveaway.message.id} due to early end.")

        # End the giveaway immediately
        await self.cog.end_giveaway(interaction.message.id, ended_by=interaction.user)
        await interaction.followup.send("Giveaway ended early.", ephemeral=True)
        logger.info(f"Giveaway {giveaway.giveaway_id}/{giveaway.message.id} ended early by {interaction.user}")


# -------------------------------------------------------------------
# Ended Giveaway View (Used after giveaway ends) - NEW CLASS
# -------------------------------------------------------------------
class EndedGiveawayView(discord.ui.View):
    def __init__(self, cog_ref, giveaway: GiveawayData): # Pass the giveaway data
        super().__init__(timeout=None)
        self.cog = cog_ref
        self.giveaway_id = giveaway.giveaway_id # Store sequential ID

        # Add Reroll Button
        self.add_item(discord.ui.Button(label="Reroll", style=discord.ButtonStyle.primary, emoji="🎲", custom_id=GIVEAWAY_REROLL_BUTTON_ID))

        # Add Link Button
        jump_url = f"https://discord.com/channels/{giveaway.guild_id}/{giveaway.channel_id}/{giveaway.message_id}"
        self.add_item(discord.ui.Button(label="View Ended Giveaway", style=discord.ButtonStyle.link, url=jump_url, emoji="🏆"))


    @discord.ui.button(label="Reroll", style=discord.ButtonStyle.primary, emoji="🎲", custom_id=GIVEAWAY_REROLL_BUTTON_ID)
    async def reroll_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check: Host or Staff Role or Manage Guild
        # Get giveaway data from cache using the stored giveaway_id
        giveaway = self.cog.get_giveaway_by_sequential_id(guild.id, self.giveaway_id)

        if not giveaway or giveaway.guild_id != guild.id:
             # This shouldn't happen if the view is attached to the correct message
             await interaction.response.send_message("Could not find data for this giveaway.", ephemeral=True)
             return

        host_id = giveaway.host_id
        member = guild.get_member(interaction.user.id)
        if not member:
            await interaction.response.send_message("Could not verify your identity.", ephemeral=True)
            return

        guild_settings = self.cog.guild_settings.get(guild.id)
        is_staff = False
        if guild_settings and guild_settings.staff_role_id:
            staff_role = guild.get_role(guild_settings.staff_role_id)
            if staff_role and staff_role in member.roles:
                 is_staff = True

        # Check if user is host OR has manage_guild OR is staff
        if interaction.user.id != host_id and not member.guild_permissions.manage_guild and not is_staff:
            perm_msg = "Only the host"
            can_list = []
            if is_staff: can_list.append("staff members")
            if member.guild_permissions.manage_guild: can_list.append("users with 'Manage Guild'")
            if can_list: perm_msg += " or " + " or ".join(can_list)
            perm_msg += " permission can reroll the giveaway."
            return await interaction.response.send_message(perm_msg, ephemeral=True)

        # Defer response, thinking=True for the reroll process
        await interaction.response.defer(ephemeral=True, thinking=True)

        # Trigger the reroll logic (reuse the core logic from the /greroll command)
        await self.cog.perform_reroll(interaction, giveaway) # Call the cog method


# -------------------------------------------------------------------
# Giveaway Cog - Main Logic (Updated)
# -------------------------------------------------------------------
class GiveawayCog(commands.Cog, name="Giveaways"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.active_giveaways: Dict[int, GiveawayData] = {} # message_id: GiveawayData (Global index for easy lookup by message ID)
        self.ended_giveaways_cache: Dict[int, GiveawayData] = {} # message_id: GiveawayData (Global cache for reroll)
        self.guild_settings: Dict[int, GuildSettings] = {} # guild_id: GuildSettings
        self.user_stats: Dict[int, Dict[int, UserGiveawayStats]] = {} # guild_id: { user_id: UserGiveawayStats } # New attribute for user stats
        # Secondary index for sequential ID lookup: (guild_id, giveaway_id) -> message_id
        self._sequential_id_map: Dict[tuple[int, int], int] = {}
        self.giveaway_end_tasks: Dict[int, asyncio.Task] = {} # message_id: end_task
        # Use NEW ActiveGiveawayView and EndedGiveawayView
        # Persistent views are registered in cog_load

        # Load state on startup
        self.load_state()

    def cog_load(self):
        # Register the persistent views ONCE when the cog loads
        self.bot.add_view(ActiveGiveawayView(self)) # Register the active view
        # EndedGiveawayView needs giveaway data to construct its persistent ID,
        # which isn't available at load time. We should register it without dummy data.
        # The persistent ID should be generated based on the custom_id constant.
        # Let's fix the EndedGiveawayView's __init__ to generate a consistent ID.
        # Re-registering with dummy data is a common workaround but can be brittle.
        # A more robust way is to ensure the persistent ID is solely based on custom_id.
        # Let's adjust EndedGiveawayView slightly if needed or rely on discord.py handling.
        # The super().__init__(timeout=None) with defined custom_ids should handle it.
        # We *do* need to pass the cog reference. Let's re-register it correctly.
        self.bot.add_view(EndedGiveawayView(self, giveaway=GiveawayData(giveaway_id=0, message_id=0, channel_id=0, guild_id=0, prize="", host_id=0, winners_count=0, start_time=datetime.now(timezone.utc), end_time=datetime.now(timezone.utc)))) # Register ended view with minimal dummy data for registration

        logger.info("Persistent GiveawayViews registered.")
        # Start the loop to check for ended giveaways missed during downtime
        self.check_missed_giveaways.start()

    def cog_unload(self):
        # Cancel all running giveaway end tasks when cog unloads
        for task in self.giveaway_end_tasks.values():
            task.cancel()
        self.check_missed_giveaways.cancel()
        logger.info("Giveaway end tasks cancelled and check loop stopped.")

    def load_state(self):
        """Loads guild settings, active giveaways, ended giveaways, and user stats from per-guild files."""
        self.active_giveaways = {}
        self.ended_giveaways_cache = {}
        self.guild_settings = {}
        self.user_stats = {} # Initialize user_stats
        self._sequential_id_map = {}

        now = datetime.now(timezone.utc)

        if not os.path.exists(STORAGE_DIR):
             logger.info(f"Storage directory '{STORAGE_DIR}' not found. No state to load.")
             return

        # Iterate through guild directories
        for item in os.listdir(STORAGE_DIR):
            guild_dir = os.path.join(STORAGE_DIR, item)
            if os.path.isdir(guild_dir):
                try:
                    guild_id = int(item)
                except ValueError:
                    logger.warning(f"Invalid directory name in storage: {item}. Skipping.")
                    continue

                # Load settings for this guild
                settings = load_guild_settings(guild_id)
                self.guild_settings[guild_id] = settings

                # Load active giveaways for this guild
                active_guild_giveaways = load_giveaways_for_guild(guild_id, is_ended=False)
                giveaways_to_remove = []

                for msg_id, giveaway in active_guild_giveaways.items():
                     if giveaway.ended:
                         giveaways_to_remove.append(msg_id)
                         continue

                     self.active_giveaways[msg_id] = giveaway
                     # Add to sequential ID map
                     if giveaway.giveaway_id:
                         self._sequential_id_map[(giveaway.guild_id, giveaway.giveaway_id)] = msg_id

                     if giveaway.end_time <= now and not giveaway.is_drop: # Only schedule standard giveaways
                         # Giveaway should have ended while bot was offline
                         logger.info(f"Giveaway {giveaway.giveaway_id}/{msg_id} in guild {guild_id} end time passed while offline. Scheduling immediate end.")
                         # Schedule to end ASAP, not blocking startup
                         asyncio.create_task(self.end_giveaway(msg_id, ended_by=self.bot.user))
                     elif not giveaway.ended and not giveaway.is_drop: # Only schedule standard giveaways if not ended
                         # Schedule the end task for giveaways still active
                         self.schedule_giveaway_end(giveaway)
                    # Note: Drop giveaways are not scheduled via timer, they end on first join


                # Remove giveaways that were somehow marked ended in the active file
                if giveaways_to_remove:
                    for msg_id in giveaways_to_remove:
                        active_guild_giveaways.pop(msg_id, None)
                    save_giveaways_for_guild(active_guild_giveaways, guild_id, is_ended=False)

                # Load ended giveaways cache for this guild
                ended_guild_giveaways = load_giveaways_for_guild(guild_id, is_ended=True)
                # Limit cache size on load if necessary
                if len(ended_guild_giveaways) > MAX_ENDED_GIVEAWAYS_STORED:
                    # Sort by end time and keep the most recent
                    sorted_ended = sorted(ended_guild_giveaways.items(), key=lambda item: item[1].end_time, reverse=True)
                    ended_guild_giveaways = dict(sorted_ended[:MAX_ENDED_GIVEAWAYS_STORED])
                    # Resave to trim
                    save_giveaways_for_guild(ended_guild_giveaways, guild_id, is_ended=True)


                for msg_id, giveaway in ended_guild_giveaways.items():
                    self.ended_giveaways_cache[msg_id] = giveaway
                    # Add to sequential ID map (even if ended, for reroll lookup)
                    if giveaway.giveaway_id:
                         self._sequential_id_map[(giveaway.guild_id, giveaway.giveaway_id)] = msg_id

                # Load user stats for this guild (New)
                guild_user_stats = load_guild_user_stats(guild_id)
                self.user_stats[guild_id] = guild_user_stats # Store user stats per guild


        logger.info(f"Initial state loaded. Active: {len(self.active_giveaways)}, Ended Cache: {len(self.ended_giveaways_cache)}, Guilds: {len(self.guild_settings)}, User Stats Guilds: {len(self.user_stats)}")


    def save_active_giveaways_for_guild(self, guild_id: int):
        """Saves active giveaways filtered by guild ID."""
        guild_active_giveaways = {msg_id: gw for msg_id, gw in self.active_giveaways.items() if gw.guild_id == guild_id and not gw.ended}
        save_giveaways_for_guild(guild_active_giveaways, guild_id, is_ended=False)

    def save_ended_giveaway_cache_for_guild(self, giveaway: GiveawayData):
        """Adds an ended giveaway to the cache file for reroll for its guild."""
        guild_id = giveaway.guild_id
        file_path = get_guild_giveaways_file(guild_id, is_ended=True)

        # Load existing cache for this guild
        guild_ended_giveaways = load_giveaways_for_guild(guild_id, is_ended=True)

        # Add/update the new ended giveaway
        guild_ended_giveaways[giveaway.message_id] = giveaway

        # Limit cache size
        if len(guild_ended_giveaways) > MAX_ENDED_GIVEAWAYS_STORED:
            # Remove the oldest ones based on end time
            sorted_ended = sorted(guild_ended_giveaways.items(), key=lambda item: item[1].end_time, reverse=True)
            guild_ended_giveaways = dict(sorted_ended[:MAX_ENDED_GIVEAWAY_STORED])

        # Save the updated cache
        save_giveaways_for_guild(guild_ended_giveaways, guild_id, is_ended=True)

        # Update global cache and map
        self.ended_giveaways_cache[giveaway.message_id] = giveaway
        if giveaway.giveaway_id:
             self._sequential_id_map[(giveaway.guild_id, giveaway.giveaway_id)] = giveaway.message_id


    def get_giveaway_by_sequential_id(self, guild_id: int, giveaway_id: int) -> Optional[GiveawayData]:
        """Looks up a giveaway (active or ended) by its sequential ID and guild ID."""
        msg_id = self._sequential_id_map.get((guild_id, giveaway_id))
        if msg_id is None:
            return None # ID not found in map

        # Check active giveaways first
        giveaway = self.active_giveaways.get(msg_id)
        if giveaway and giveaway.guild_id == guild_id:
            return giveaway

        # Check ended cache
        giveaway = self.ended_giveaways_cache.get(msg_id)
        if giveaway and giveaway.guild_id == guild_id:
             return giveaway

        # If found in map but not in cache/active (data inconsistency), rebuild map?
        # For now, just return None.
        logger.warning(f"Sequential ID {giveaway_id} for guild {guild_id} found in map, but message ID {msg_id} not found in active or ended cache.")
        return None


    def schedule_giveaway_end(self, giveaway: GiveawayData):
        """Schedules the asyncio task to end a specific giveaway."""
        # Only schedule standard giveaways, not drops
        if giveaway.is_drop:
             logger.debug(f"Not scheduling end task for drop giveaway {giveaway.giveaway_id}/{giveaway.message_id}.")
             return

        if giveaway.message_id in self.giveaway_end_tasks and not self.giveaway_end_tasks[giveaway.message_id].done():
            logger.warning(f"End task for giveaway {giveaway.giveaway_id}/{giveaway.message_id} already exists. Skipping schedule.")
            return

        now = datetime.now(timezone.utc)
        delay = (giveaway.end_time - now).total_seconds()

        if delay <= 0:
            logger.warning(f"Attempted to schedule end for giveaway {giveaway.giveaway_id}/{giveaway.message_id} that should have already ended. Ending now.")
            # Run immediately in background
            task = asyncio.create_task(self.end_giveaway(giveaway.message_id, ended_by=self.bot.user))
        else:
            logger.info(f"Scheduling end for giveaway {giveaway.giveaway_id}/{giveaway.message_id} in {delay:.2f} seconds.")
            task = asyncio.create_task(self.giveaway_end_runner(giveaway.message_id, delay))

        self.giveaway_end_tasks[giveaway.message_id] = task
        giveaway.task_scheduled = True # Mark task as scheduled


    async def giveaway_end_runner(self, message_id: int, delay: float):
        """The coroutine that waits and then calls end_giveaway."""
        try:
            await asyncio.sleep(delay)
            logger.info(f"Timer finished for giveaway message {message_id}. Triggering end.")
            # Fetch the giveaway data again in case it was modified
            giveaway = self.active_giveaways.get(message_id)
            if giveaway and not giveaway.is_drop: # Ensure it's a standard giveaway
                await self.end_giveaway(message_id, ended_by=self.bot.user)
            elif giveaway and giveaway.is_drop:
                 logger.debug(f"End timer triggered for drop giveaway {giveaway.giveaway_id}, but drops end on first join. Skipping timer end.")
            else:
                logger.warning(f"Giveaway message {message_id} not found in active list when end runner triggered. Already ended or removed?")

        except asyncio.CancelledError:
            logger.info(f"End task for giveaway message {message_id} was cancelled.")
        except Exception as e:
            logger.error(f"Error in giveaway end runner for message {message_id}: {e}", exc_info=True)
        finally:
            # Clean up the task reference once it's done or cancelled
            self.giveaway_end_tasks.pop(message_id, None)


    # Add instant_winner parameter for drops
    async def end_giveaway(self, message_id: int, ended_by: Optional[discord.User | discord.Member] = None, instant_winner: Optional[int] = None):
        """Handles the logic for ending a giveaway, finding winners, and updating messages."""
        giveaway = self.active_giveaways.get(message_id)
        if not giveaway:
            logger.warning(f"Attempted to end non-existent or already ended giveaway message {message_id}.")
            giveaway = self.ended_giveaways_cache.get(message_id)
            if not giveaway or giveaway.ended:
                 # Cleanup task if it somehow persisted
                 self.giveaway_end_tasks.pop(message_id, None)
                 return # Avoid double processing

        if giveaway.ended:
             logger.warning(f"Giveaway {giveaway.giveaway_id}/{message_id} processing end, but already marked as ended.")
             self.giveaway_end_tasks.pop(message_id, None)
             return # Avoid double processing


        logger.info(f"Ending giveaway {giveaway.giveaway_id}/{message_id} (Prize: {giveaway.prize}). Ended by: {ended_by or 'Scheduled Task'}")
        giveaway.ended = True
        giveaway.task_scheduled = False

        # Remove from active giveaways (global dict) and save for this guild
        self.active_giveaways.pop(message_id, None)
        self.save_active_giveaways_for_guild(giveaway.guild_id)

        # Add to ended cache and save for this guild
        self.save_ended_giveaway_cache_for_guild(giveaway)

        # --- Find Winners ---
        winners = []
        eligible_participants = []

        guild = self.bot.get_guild(giveaway.guild_id)
        guild_settings = self.guild_settings.get(giveaway.guild_id) or GuildSettings(giveaway.guild_id) # Get settings or default

        if instant_winner: # Special case for drop giveaways
             if instant_winner in giveaway.participants:
                  winners.append(instant_winner)
                  logger.info(f"Drop giveaway {giveaway.giveaway_id} won instantly by {instant_winner}.")
             else:
                 logger.warning(f"Instant winner {instant_winner} for drop {giveaway.giveaway_id} not found in participants.")
                 # Should not happen if logic is correct, but handle defensively

        elif guild: # Normal giveaway winner drawing
            participants_list = list(giveaway.participants.keys())
            if participants_list:
                # Filter participants based on blacklist/bypass roles at the time of ending
                all_blacklist_roles = set()
                if giveaway.blacklist_role_id:
                     all_blacklist_roles.add(giveaway.blacklist_role_id)
                if guild_settings and guild_settings.default_blacklist_role_id:
                     all_blacklist_roles.add(guild_settings.default_blacklist_role_id)

                all_bypass_roles = set(giveaway.bypass_role_ids)
                if guild_settings and guild_settings.default_bypass_role_ids:
                     all_bypass_roles.update(guild_settings.default_bypass_role_ids)


                for user_id in participants_list:
                     member = guild.get_member(user_id)
                     if not member:
                         logger.warning(f"Participant {user_id} not found in guild {guild.id} during winner drawing for giveaway {giveaway.giveaway_id}. Skipping.")
                         continue # Skip if user is no longer in the guild

                     member_roles_set = {role.id for role in member.roles}
                     has_bypass = any(role_id in all_bypass_roles for role_id in member_roles_set)
                     is_blacklisted = any(role_id in all_blacklist_roles for role_id in member_roles_set)

                     if not is_blacklisted or has_bypass:
                          eligible_participants.append(user_id)

            if eligible_participants:
                # Create weighted list from eligible participants
                entries_weighted_list = []
                for user_id in eligible_participants:
                     if user_id in giveaway.participants:
                         entries_weighted_list.extend([user_id] * giveaway.participants[user_id])

                actual_winner_count = min(giveaway.winners_count, len(set(eligible_participants)))

                if actual_winner_count > 0 and entries_weighted_list:
                     drawn_winners = set()
                     attempts = 0
                     max_attempts = actual_winner_count * 10

                     while len(drawn_winners) < actual_winner_count and attempts < max_attempts:
                          if not entries_weighted_list: break # No more entries to draw from
                          chosen_user_id = random.choice(entries_weighted_list)
                          if chosen_user_id not in drawn_winners:
                               drawn_winners.add(chosen_user_id)
                          attempts += 1
                     winners = list(drawn_winners)

        # --- Increment User Win Stats ---
        if winners:
            # Load stats for the guild if not already cached (should be by load_state)
            if giveaway.guild_id not in self.user_stats:
                 self.user_stats[giveaway.guild_id] = load_guild_user_stats(giveaway.guild_id)
            guild_stats = self.user_stats[giveaway.guild_id]

            now = datetime.now(timezone.utc)
            for winner_id in winners:
                 if winner_id not in guild_stats:
                      guild_stats[winner_id] = UserGiveawayStats(user_id=winner_id, guild_id=giveaway.guild_id)
                 guild_stats[winner_id].won_count += 1
                 guild_stats[winner_id].won_last_timestamp = now

            save_guild_user_stats(guild_stats, giveaway.guild_id)


        # --- Update Original Message ---
        channel = self.bot.get_channel(giveaway.channel_id)
        original_msg = None
        if channel:
            try:
                # Pass guild_settings to embed function
                ended_embed_status = "ended_no_winners" if not winners else "ended"
                ended_embed = create_giveaway_embed(giveaway, self.bot, status=ended_embed_status, guild_settings=guild_settings)

                # Add winner info to embed
                winner_mentions = []
                if winners:
                     for winner_id in winners:
                         winner_user = self.bot.get_user(winner_id) or await self.bot.fetch_user(winner_id) # Fetch if not cached
                         winner_mentions.append(winner_user.mention if winner_user else f"User ID: {winner_id}")
                     ended_embed.add_field(name="🏆 Winner(s)", value=", ".join(winner_mentions), inline=False)
                else:
                     ended_embed.add_field(name="🏆 Winner(s)", value="No eligible participants found!", inline=False)

                # Edit the message with the ended embed and the NEW EndedGiveawayView
                original_msg = await channel.fetch_message(message_id)
                ended_view = EndedGiveawayView(self, giveaway=giveaway) # Create instance of the new view
                await original_msg.edit(embed=ended_embed, view=ended_view) # Replace the view

            except discord.NotFound:
                logger.error(f"Original giveaway message {message_id} not found in channel {giveaway.channel_id} during end process.")
            except discord.Forbidden:
                 logger.error(f"Bot lacks permission to edit message {message_id} or read channel {giveaway.channel_id} during end process.")
            except Exception as e:
                logger.error(f"Error updating original giveaway message {message_id}: {e}", exc_info=True)
        else:
            logger.error(f"Giveaway channel {giveaway.channel_id} not found for ending message {message_id}.")


        # --- Announce Winners ---
        if channel and original_msg:
            try:
                if winners:
                    winner_mentions_str = ", ".join(winner_mentions)
                    # Use customizable win message
                    win_message = guild_settings.win_message.format(winners=winner_mentions_str, prize=giveaway.prize)
                    await channel.send(
                        win_message,
                        reference=original_msg,
                        allowed_mentions=discord.AllowedMentions(users=True) # Ensure winners are pinged
                    )
                    logger.info(f"Announced winners for giveaway {giveaway.giveaway_id}/{message_id}: {winner_mentions_str}")
                    await self.log_giveaway_event("end_winners", giveaway, ended_by, winner_ids=winners) # Pass ended_by

                    # --- DM Winners (if enabled) ---
                    if guild_settings.dm_winner:
                         await self.dm_giveaway_winners(guild, winners, giveaway, guild_settings)

                else:
                     # Use customizable no winners message
                     nowinners_message = guild_settings.nowinners_message.format(prize=giveaway.prize)
                     await channel.send(
                        nowinners_message,
                        reference=original_msg
                    )
                     logger.info(f"Giveaway {giveaway.giveaway_id}/{message_id} ended with no winners.")
                     await self.log_giveaway_event("end_no_winners", giveaway, ended_by) # Pass ended_by

            except discord.Forbidden:
                 logger.error(f"Bot lacks permission to send messages in {giveaway.channel_id} for winner announcement.")
            except Exception as e:
                 logger.error(f"Error sending winner announcement for {message_id}: {e}", exc_info=True)

        # Final cleanup of task reference
        self.giveaway_end_tasks.pop(message_id, None)

    # --- New function to send DM to winners ---
    async def dm_giveaway_winners(self, guild: discord.Guild, winner_ids: List[int], giveaway: GiveawayData, settings: GuildSettings):
        """Sends a DM embed to each winner."""
        if not settings.dm_winner: return # Double check setting

        for winner_id in winner_ids:
            try:
                user = self.bot.get_user(winner_id) or await self.bot.fetch_user(winner_id)
                if not user:
                    logger.warning(f"Could not fetch user {winner_id} to DM for giveaway {giveaway.giveaway_id}.")
                    continue

                # Get DM embed color (random or hex)
                dm_color = discord.Color.random() if settings.colour_dm_winembed.lower() == 'random' else discord.Color.blue() # Default blue if hex invalid
                if settings.colour_dm_winembed.startswith('#'):
                     try:
                         dm_color = discord.Color.from_rgb(*tuple(int(settings.colour_dm_winembed[i:i+2], 16) for i in (1, 3, 5)))
                     except:
                         logger.warning(f"Invalid hex color for winner DM embed in guild {settings.guild_id}: {settings.colour_dm_winembed}. Using default blue.")
                         dm_color = discord.Color.blue()


                # Create DM embed using customizable settings
                dm_embed = discord.Embed(
                    title=settings.title_dm_winembed.format(prize=giveaway.prize, guild_name=guild.name),
                    description=settings.description_dm_winembed.format(prize=giveaway.prize, guild_name=guild.name),
                    color=dm_color,
                    timestamp=datetime.now(timezone.utc)
                )
                if settings.thumbnail_dm_winembed:
                    dm_embed.set_thumbnail(url=settings.thumbnail_dm_winembed)
                if settings.footer_dm_winembed:
                    # No custom emoji in footer text
                    dm_embed.set_footer(text=settings.footer_dm_winembed.format(giveaway_id=giveaway.giveaway_id))

                # Add field linking to the giveaway message
                jump_url = f"https://discord.com/channels/{giveaway.guild_id}/{giveaway.channel_id}/{giveaway.message_id}"
                dm_embed.add_field(name="Giveaway Link", value=f"[Jump to the giveaway message]({jump_url})", inline=False)
                dm_embed.add_field(name="Prize", value=giveaway.prize, inline=False)
                host = guild.get_member(giveaway.host_id)
                dm_embed.add_field(name="Hosted By", value=host.mention if host else f"ID: {giveaway.host_id}", inline=False)


                await user.send(embed=dm_embed)
                logger.info(f"Sent win DM to user {user.id} for giveaway {giveaway.giveaway_id}.")
            except discord.Forbidden:
                logger.warning(f"Could not send DM to user {winner_id} for giveaway {giveaway.giveaway_id} (DMs blocked).")
            except Exception as e:
                logger.error(f"Failed to send DM to user {winner_id} for giveaway {giveaway.giveaway_id}: {e}", exc_info=True)

    # --- New function to send DM to host for start confirmation ---
    async def dm_giveaway_host(self, guild: discord.Guild, host_id: int, giveaway: GiveawayData, settings: GuildSettings):
         """Sends a DM embed to the host when their giveaway starts."""
         try:
             user = self.bot.get_user(host_id) or await self.bot.fetch_user(host_id)
             if not user:
                 logger.warning(f"Could not fetch host user {host_id} to DM for giveaway {giveaway.giveaway_id}.")
                 return

             # Get DM embed color (random or hex)
             dm_color = discord.Color.random() if settings.colour_dm_hostembed.lower() == 'random' else discord.Color.blue() # Default blue if hex invalid
             if settings.colour_dm_hostembed.startswith('#'):
                  try:
                      dm_color = discord.Color.from_rgb(*tuple(int(settings.colour_dm_hostembed[i:i+2], 16) for i in (1, 3, 5)))
                  except:
                      logger.warning(f"Invalid hex color for host DM embed in guild {settings.guild_id}: {settings.colour_dm_hostembed}. Using default blue.")
                      dm_color = discord.Color.blue()


             # Create DM embed using customizable settings
             dm_embed = discord.Embed(
                 title=settings.title_dm_hostembed.format(prize=giveaway.prize, guild_name=guild.name),
                 description=settings.description_dm_hostembed.format(prize=giveaway.prize, guild_name=guild.name),
                 color=dm_color,
                 timestamp=datetime.now(timezone.utc)
             )
             if settings.thumbnail_dm_hostembed:
                 dm_embed.set_thumbnail(url=settings.thumbnail_dm_hostembed)
             if settings.footer_dm_hostembed:
                 # No custom emoji in footer text
                 dm_embed.set_footer(text=settings.footer_dm_hostembed.format(giveaway_id=giveaway.giveaway_id))

             # Add field linking to the giveaway message
             jump_url = f"https://discord.com/channels/{giveaway.guild_id}/{giveaway.channel_id}/{giveaway.message_id}"
             dm_embed.add_field(name="Giveaway Link", value=f"[Jump to the giveaway message]({jump_url})", inline=False)
             dm_embed.add_field(name="Prize", value=giveaway.prize, inline=False)
             dm_embed.add_field(name="Ends", value=f"<t:{int(giveaway.end_time.timestamp())}:R>", inline=False)


             await user.send(embed=dm_embed)
             logger.info(f"Sent host DM to user {host_id} for giveaway {giveaway.giveaway_id}.")
         except discord.Forbidden:
             logger.warning(f"Could not send DM to host {host_id} for giveaway {giveaway.giveaway_id} (DMs blocked).")
         except Exception as e:
             logger.error(f"Failed to send DM to host {host_id} for giveaway {giveaway.giveaway_id}: {e}", exc_info=True)


    # --- New function to perform the core reroll logic ---
    # This will be called by both the /greroll command and the Reroll button
    async def perform_reroll(self, interaction: discord.Interaction, giveaway: GiveawayData):
        """Performs the core logic of rerolling winners for an ended giveaway."""
        guild = interaction.guild
        guild_settings = self.guild_settings.get(guild.id) or GuildSettings(guild.id)

        # --- Get Eligible Participants ---
        participants_list = list(giveaway.participants.keys())
        if not participants_list:
            await interaction.followup.send("Cannot reroll: No participants were recorded for this giveaway.", ephemeral=True)
            return

        eligible_participants = []

        # Filter participants based on blacklist/bypass roles at the time of rerolling (using current roles)
        all_blacklist_roles = set()
        if giveaway.blacklist_role_id:
             all_blacklist_roles.add(giveaway.blacklist_role_id)
        if guild_settings and guild_settings.default_blacklist_role_id:
             all_blacklist_roles.add(guild_settings.default_blacklist_role_id)

        all_bypass_roles = set(giveaway.bypass_role_ids)
        if guild_settings and guild_settings.default_bypass_role_ids:
             all_bypass_roles.update(guild_settings.default_bypass_role_ids)

        for user_id in participants_list:
             member = guild.get_member(user_id)
             if not member:
                 logger.warning(f"Participant {user_id} not found in guild {guild.id} during reroll for giveaway {giveaway.giveaway_id}. Skipping.")
                 continue

             member_roles_set = {role.id for role in member.roles}
             has_bypass = any(role_id in all_bypass_roles for role_id in member_roles_set)
             is_blacklisted = any(role_id in all_blacklist_roles for role_id in member_roles_set)

             if not is_blacklisted or has_bypass:
                  eligible_participants.append(user_id)


        if not eligible_participants:
             await interaction.followup.send("Cannot reroll: No eligible participants remaining (all might have won already or left, or are now blacklisted).", ephemeral=True)
             return

        # Create weighted list from eligible participants
        entries_weighted_list = []
        # For reroll, we should use the original entries if it's a normal giveaway.
        # For drops, everyone has 1 entry.
        if not giveaway.is_drop:
             for user_id in eligible_participants:
                  if user_id in giveaway.participants:
                      entries_weighted_list.extend([user_id] * giveaway.participants[user_id])
        else: # For drops, eligible participants just have 1 entry each
             entries_weighted_list = list(eligible_participants)


        num_to_reroll = min(giveaway.winners_count, len(set(eligible_participants)))

        if num_to_reroll <=0 or not entries_weighted_list:
             await interaction.followup.send("Cannot reroll: No winners needed or no eligible participants left with entries.", ephemeral=True)
             return

        new_winners = set()
        attempts = 0
        max_attempts = num_to_reroll * 10 # Safety break
        while len(new_winners) < num_to_reroll and attempts < max_attempts:
             if not entries_weighted_list: break
             chosen_user_id = random.choice(entries_weighted_list)
             new_winners.add(chosen_user_id)
             attempts += 1

        winners = list(new_winners)

        if not winners:
             await interaction.followup.send("Failed to select new winners after rerolling.", ephemeral=True)
             return

        # --- Increment User Win Stats (for rerolled winners) ---
        if winners:
            if giveaway.guild_id not in self.user_stats:
                 self.user_stats[giveaway.guild_id] = load_guild_user_stats(giveaway.guild_id)
            guild_stats = self.user_stats[giveaway.guild_id]

            now = datetime.now(timezone.utc)
            for winner_id in winners:
                 if winner_id not in guild_stats:
                      guild_stats[winner_id] = UserGiveawayStats(user_id=winner_id, guild_id=giveaway.guild_id)
                 # Decide how to handle reroll stats: Increment 'won_count' again? Or a separate rerolled count?
                 # User requested only win/hosted/donated. Let's increment 'won_count' and update last_timestamp.
                 guild_stats[winner_id].won_count += 1
                 guild_stats[winner_id].won_last_timestamp = now

            save_guild_user_stats(guild_stats, giveaway.guild_id)


        # --- Announce Rerolled Winners ---
        channel = self.bot.get_channel(giveaway.channel_id)
        original_msg = None
        if channel:
             try:
                 original_msg = await channel.fetch_message(giveaway.message_id)
             except Exception as e:
                 logger.warning(f"Could not fetch original message {giveaway.message_id} for reroll announcement: {e}")

        if channel and original_msg:
            reroll_mentions = []
            for winner_id in winners:
                winner_user = guild.get_member(winner_id) or await self.bot.fetch_user(winner_id)
                reroll_mentions.append(winner_user.mention if winner_user else f"User ID: {winner_id}")

            try:
                # Use customizable reroll message
                reroll_message_text = guild_settings.reroll_message.format(winners=', '.join(reroll_mentions), prize=giveaway.prize)
                reroll_view = EndedGiveawayView(self, giveaway=giveaway) # Use the ended view with link

                await channel.send(
                    reroll_message_text,
                    reference=original_msg,
                    view=reroll_view, # Keep the view for consistency
                    allowed_mentions=discord.AllowedMentions(users=True)
                )
                await interaction.followup.send(f"✅ Rerolled winners for giveaway ID **{giveaway.giveaway_id}**.", ephemeral=True)
                logger.info(f"Rerolled winners for {giveaway.giveaway_id}/{giveaway.message_id}: {', '.join(reroll_mentions)}")
                await self.log_giveaway_event("reroll", giveaway, interaction.user, winner_ids=winners)

                 # --- DM Rerolled Winners (if enabled) ---
                if guild_settings.dm_winner:
                     await self.dm_giveaway_winners(guild, winners, giveaway, guild_settings)


            except discord.Forbidden:
                await interaction.followup.send(f"✅ Rerolled winners for {giveaway.giveaway_id}, but I lack permission to announce them in the channel.", ephemeral=True)
                await self.log_giveaway_event("reroll", giveaway, interaction.user, winner_ids=winners)
            except Exception as e:
                logger.error(f"Error sending reroll announcement for {giveaway.message_id}: {e}", exc_info=True)
                await interaction.followup.send(f"✅ Rerolled winners for {giveaway.giveaway_id}, but failed to send announcement: {e}", ephemeral=True)
                await self.log_giveaway_event("reroll", giveaway, interaction.user, winner_ids=winners)
        else:
             await interaction.followup.send(f"Could not find the original giveaway message/channel ({giveaway.message_id}) to announce the reroll for giveaway ID **{giveaway.giveaway_id}**.", ephemeral=True)
             await self.log_giveaway_event("reroll", giveaway, interaction.user, winner_ids=winners)


    # --- Giveaway Logging (Updated) ---
    async def log_giveaway_event(self, event_type: str, giveaway: GiveawayData, user: Optional[Union[discord.User, discord.Member]] = None, winner_ids: Optional[List[int]] = None):
        """Sends a detailed log embed to the configured log channel for specific event types."""
        # Only log these specific event types
        if event_type not in ["start", "end_winners", "end_no_winners", "cancel", "reroll"]:
             return # Do not log other events like join/leave

        guild = self.bot.get_guild(giveaway.guild_id)
        if not guild or guild.id not in self.guild_settings or not self.guild_settings[guild.id].log_channel_id:
            return # No guild, no settings, or no log channel configured

        log_channel = guild.get_channel(self.guild_settings[guild.id].log_channel_id)
        if not isinstance(log_channel, discord.TextChannel):
             logger.warning(f"Configured log channel ID {self.guild_settings[guild.id].log_channel_id} for guild {guild.id} is not a valid text channel.")
             return

        bot_member = guild.get_member(self.bot.user.id)
        if not bot_member or not log_channel.permissions_for(bot_member).send_messages or not log_channel.permissions_for(bot_member).embed_links:
             logger.error(f"Bot lacks permissions to send messages/embeds in the log channel {log_channel.id} for guild {guild.id}.")
             return

        embed_title = "Giveaway Event Log"
        color = discord.Color.light_grey()
        description = f"Giveaway **{giveaway.giveaway_id}** (Prize: {giveaway.prize})"
        fields = []

        host = guild.get_member(giveaway.host_id) or f"ID: {giveaway.host_id}"
        fields.append({"name": "Hosted by", "value": host.mention if isinstance(host, (discord.User, discord.Member)) else host, "inline": True})
        fields.append({"name": "Channel", "value": guild.get_channel(giveaway.channel_id).mention if guild.get_channel(giveaway.channel_id) else f"ID: {giveaway.channel_id}", "inline": True})
        fields.append({"name": "Message ID", "value": str(giveaway.message_id), "inline": True})
        fields.append({"name": "Sequential ID", "value": str(giveaway.giveaway_id), "inline": True})
        if giveaway.is_drop: # Add drop indicator
             fields.append({"name": "Type", "value": "Drop Giveaway", "inline": True})


        if event_type == "start":
            embed_title = "🎉 Giveaway Started"
            color = discord.Color.green()
            fields.append({"name": "Duration", "value": f"<t:{int(giveaway.end_time.timestamp())}:R>", "inline": True})
            fields.append({"name": "Winners", "value": str(giveaway.winners_count), "inline": True})
            if user:
                 fields.append({"name": "Started by", "value": user.mention, "inline": True})
            req_details = []
            if giveaway.required_role_id: req_details.append(f"Required Role: {guild.get_role(giveaway.required_role_id).mention if guild and guild.get_role(giveaway.required_role_id) else f'ID:{giveaway.required_role_id}'}")
            if giveaway.min_messages > 0:
                count_channel = guild.get_channel(giveaway.message_count_channel_id) if guild and giveaway.message_count_channel_id else guild.get_channel(giveaway.channel_id) if guild else None
                channel_mention = count_channel.mention if count_channel else 'Unknown Channel'
                req_text = f"Min Msgs ({giveaway.min_messages}) in {channel_mention}"
                if giveaway.message_cooldown_seconds > 0: req_text += f" ({giveaway.message_cooldown_seconds}s cooldown)"
                if giveaway.required_keywords: req_text += f" (Keywords: {', '.join(giveaway.required_keywords)})"
                req_details.append(req_text)
            if giveaway.bonus_entries:
                bonus_list = ", ".join(f"{guild.get_role(rid).mention if guild and guild.get_role(rid) else f'ID:{rid}'}:{extra}" for rid, extra in giveaway.bonus_entries.items())
                req_details.append(f"Bonus Entries: {bonus_list}")
            all_bypass_roles = set(giveaway.bypass_role_ids)
            if guild_settings := self.guild_settings.get(guild.id):
                all_bypass_roles.update(guild_settings.default_bypass_role_ids)
            if all_bypass_roles:
                 bypass_list = ", ".join(guild.get_role(rid).mention if guild and guild.get_role(rid) else f'ID:{rid}' for rid in all_bypass_roles)
                 req_details.append(f"Bypass Roles: {bypass_list}")
            all_blacklist_roles = set()
            if giveaway.blacklist_role_id: all_blacklist_roles.add(giveaway.blacklist_role_id)
            if guild_settings := self.guild_settings.get(guild.id):
                 if guild_settings.default_blacklist_role_id: all_blacklist_roles.add(guild_settings.default_blacklist_role_id)
            if all_blacklist_roles:
                 blacklist_list = ", ".join(guild.get_role(rid).mention if guild and guild.get_role(rid) else f'ID:{rid}' for rid in all_blacklist_roles)
                 req_details.append(f"Blacklist Roles: {blacklist_list}")
            if req_details:
                fields.append({"name": "Giveaway Details", "value": "\n".join(req_details), "inline": False})

            # Send Host DM
            try:
                guild_settings_for_dm = self.guild_settings.get(giveaway.guild_id)
                if guild and user and guild_settings_for_dm: # Ensure guild, user (starter), and settings exist
                     await self.dm_giveaway_host(guild, user.id, giveaway, guild_settings_for_dm)
            except Exception as e:
                 logger.error(f"Failed to send host DM after giveaway start {giveaway.giveaway_id}: {e}", exc_info=True)


        elif event_type == "end_winners":
            embed_title = "🏆 Giveaway Ended - Winners Drawn"
            color = discord.Color.gold()
            if winner_ids:
                 winner_mentions = []
                 for wid in winner_ids:
                     w_user = guild.get_member(wid) or await self.bot.fetch_user(wid)
                     winner_mentions.append(w_user.mention if w_user else f"ID: {wid}")
                 fields.append({"name": "Winner(s)", "value": ", ".join(winner_mentions), "inline": False})
            fields.append({"name": "Participants", "value": str(len(giveaway.participants)), "inline": True})
            if user:
                 fields.append({"name": "Ended by", "value": user.mention, "inline": True})


        elif event_type == "end_no_winners":
            embed_title = "🙁 Giveaway Ended - No Winners"
            color = discord.Color.orange()
            fields.append({"name": "Reason", "value": "No eligible participants.", "inline": False})
            fields.append({"name": "Participants", "value": str(len(giveaway.participants)), "inline": True})
            if user:
                 fields.append({"name": "Ended by", "value": user.mention, "inline": True})

        elif event_type == "cancel":
            embed_title = "❌ Giveaway Cancelled"
            color = discord.Color.dark_red()
            if user:
                 fields.append({"name": "Cancelled by", "value": user.mention, "inline": True})
            fields.append({"name": "Participants", "value": str(len(giveaway.participants)), "inline": True})


        elif event_type == "reroll":
            embed_title = "🎲 Giveaway Rerolled"
            color = discord.Color.purple()
            if winner_ids:
                 winner_mentions = []
                 for wid in winner_ids:
                     w_user = guild.get_member(wid) or await self.bot.fetch_user(wid)
                     winner_mentions.append(w_user.mention if w_user else f"ID: {wid}")
                 fields.append({"name": "New Winner(s)", "value": ", ".join(winner_mentions), "inline": False})
            if user:
                 fields.append({"name": "Rerolled by", "value": user.mention, "inline": True})
            fields.append({"name": "Original Participants", "value": str(len(giveaway.participants)), "inline": True})


        else:
            # This case should not be reached due to the initial check, but keep defensively
            return

        log_embed = discord.Embed(
            title=embed_title,
            description=description,
            color=color,
            timestamp=datetime.now(timezone.utc)
        )
        for field in fields:
            log_embed.add_field(**field)

        if giveaway.message_id:
            try:
                 jump_url = f"https://discord.com/channels/{giveaway.guild_id}/{giveaway.channel_id}/{giveaway.message_id}"
                 log_embed.add_field(name="Giveaway Message", value=f"[Jump to Message]({jump_url})", inline=False)
            except:
                 pass # Ignore if link construction fails

        try:
            await log_channel.send(embed=log_embed)
        except Exception as e:
            logger.error(f"Failed to send log embed to channel {log_channel.id} for guild {guild.id}: {e}", exc_info=True)


    # --- Periodic Check Task ---
    @tasks.loop(minutes=5) # Check periodically for ended giveaways missed during downtime
    async def check_missed_giveaways(self):
        logger.debug("Running periodic check for missed giveaways...")
        now = datetime.now(timezone.utc)
        giveaways_to_end_now = []

        # Iterate through active giveaways globally
        for msg_id, giveaway in list(self.active_giveaways.items()):
             # Only process standard giveaways for timer end
             if not giveaway.ended and not giveaway.is_drop and giveaway.end_time <= now:
                 # Check if task is already running or scheduled (it shouldn't be if end_time passed)
                 task = self.giveaway_end_tasks.get(msg_id)
                 if task and not task.done():
                      logger.warning(f"Missed giveaway check: Task for {giveaway.giveaway_id}/{msg_id} is running/scheduled despite end time passing. Skipping.")
                      continue

                 logger.info(f"Missed giveaway check: Found giveaway {giveaway.giveaway_id}/{msg_id} in guild {giveaway.guild_id} whose end time ({giveaway.end_time}) has passed. Scheduling immediate end.")
                 giveaways_to_end_now.append(msg_id)

        # Schedule end tasks for those found
        for msg_id in giveaways_to_end_now:
             # Ensure we don't double-process if end_giveaway was already called by load_state
             if msg_id in self.active_giveaways and not self.active_giveaways[msg_id].ended:
                 # Run as a new task to avoid blocking the loop
                 asyncio.create_task(self.end_giveaway(msg_id, ended_by=self.bot.user))
             else:
                 logger.debug(f"Missed giveaway check: Skipping {msg_id} as it's no longer active or already marked ended.")


    @check_missed_giveaways.before_loop
    async def before_check_missed_giveaways(self):
        await self.bot.wait_until_ready()
        logger.info("Starting periodic check for missed giveaways.")


    # --- Helper functions for settings autocomplete ---
    async def role_autocomplete(self, interaction: discord.Interaction, current: str):
        """Autocomplete for role arguments in settings."""
        if not interaction.guild: return []
        roles = interaction.guild.roles
        # Include a dummy option to unset (value 0)
        choices = [app_commands.Choice(name="Unset (clears the role)", value=0)]
        choices.extend([
            app_commands.Choice(name=role.name, value=role.id)
            for role in roles if current.lower() in role.name.lower() and role.name != "@everyone"
        ])
        # Discord autocomplete has a limit (usually 25)
        return choices[:25]

    async def channel_autocomplete(self, interaction: discord.Interaction, current: str):
        """Autocomplete for text channel arguments in settings."""
        if not interaction.guild: return []
        channels = interaction.guild.text_channels
        # Include a dummy option to unset (value 0)
        choices = [app_commands.Choice(name="Unset (clears the channel)", value=0)]
        choices.extend([
             app_commands.Choice(name=channel.name, value=channel.id)
             for channel in channels if current.lower() in channel.name.lower()
        ])
        return choices[:25]


    # -----------------------------
    # --- Slash Commands (Group Renamed) ---
    # -----------------------------
    # Rename the command group
    g_group = app_commands.Group(name="g", description="Manage giveaways and drops")

    @g_group.command(name="start", description="Start a new standard giveaway.")
    @app_commands.describe(
        duration="Duration (e.g., 10m, 1h30m, 2d).",
        winners="Number of winners (e.g., 1).",
        prize="The prize for the giveaway.",
        channel="Channel to post the giveaway in (defaults to current).",
        required_role="Role required to enter.",
        bonus_roles="Bonus entries (e.g., @Role1:2 @Role2:1). Mention roles.",
        bypass_roles="Roles that bypass requirements (e.g., @Admin @Mod). Mention roles.",
        blacklist_role="Users with this role cannot enter (unless bypassed). Mention role.",
        min_messages="Min messages sent in counting channel since giveaway start.",
        message_channel="Channel to count messages in (defaults to giveaway channel).",
        message_cooldown="Cooldown between counted messages (e.g., 30s).",
        keywords="Comma-separated keywords required in messages (e.g., enter, win).",
        donor="User who donated the prize.",
        image_url="URL of an image for the embed."
    )
    # Use the staff role check if configured, otherwise require manage_guild
    @app_commands.checks.has_permissions(manage_guild=True) # Default check, can be overridden by guild settings
    async def gstart_command(self, interaction: discord.Interaction,
                             duration: str, winners: app_commands.Range[int, 1], prize: str,
                             channel: Optional[discord.TextChannel] = None,
                             required_role: Optional[discord.Role] = None,
                             bonus_roles: Optional[str] = None,
                             bypass_roles: Optional[str] = None,
                             blacklist_role: Optional[discord.Role] = None,
                             min_messages: Optional[app_commands.Range[int, 0]] = 0,
                             message_channel: Optional[discord.TextChannel] = None,
                             message_cooldown: Optional[str] = None,
                             keywords: Optional[str] = None,
                             donor: Optional[discord.User] = None,
                             image_url: Optional[str] = None):
        """Starts a standard giveaway with various options."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check using guild settings if available
        guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id)) # Load if not cached
        self.guild_settings[guild.id] = guild_settings # Cache it
        member = guild.get_member(interaction.user.id)

        is_staff = False
        if guild_settings.staff_role_id and member:
             staff_role = guild.get_role(guild_settings.staff_role_id)
             if staff_role and staff_role in member.roles:
                  is_staff = True

        if not member.guild_permissions.manage_guild and not is_staff:
            await interaction.response.send_message("You need the 'Manage Guild' permission or the configured staff role to use this command.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        target_channel = channel or interaction.channel
        if not isinstance(target_channel, discord.TextChannel):
            await interaction.followup.send("Invalid channel selected.", ephemeral=True)
            return

        # Validate permissions in target channel
        perms = target_channel.permissions_for(guild.me)
        if not perms.send_messages or not perms.embed_links:
            await interaction.followup.send(f"I need 'Send Messages' and 'Embed Links' permissions in {target_channel.mention}.", ephemeral=True)
            return

        # Validate permissions in message counting channel if specified
        count_channel = message_channel or target_channel
        if not isinstance(count_channel, discord.TextChannel): # Should not happen if target_channel is valid
             await interaction.followup.send("Invalid message counting channel.", ephemeral=True)
             return
        count_perms = count_channel.permissions_for(guild.me)
        if min_messages > 0 and not count_perms.read_message_history:
             await interaction.followup.send(f"I need 'Read Message History' permission in {count_channel.mention} to check message requirements.", ephemeral=True)
             return


        delta = parse_duration(duration)
        if delta is None:
            await interaction.followup.send("Invalid duration format. Use s, m, h, d (e.g., 30s, 15m, 1h, 2d).", ephemeral=True)
            return

        cooldown_seconds = 0
        if message_cooldown:
             cooldown_delta = parse_duration(message_cooldown)
             if cooldown_delta is None:
                  await interaction.followup.send("Invalid message cooldown format. Use s, m, h (e.g., 30s, 5m).", ephemeral=True)
                  return
             cooldown_seconds = int(cooldown_delta.total_seconds())
             if cooldown_seconds < 0: cooldown_seconds = 0 # Ensure non-negative

        start_time = datetime.now(timezone.utc)
        end_time = start_time + delta

        # Get the next sequential giveaway ID for this guild
        sequential_id = guild_settings.next_giveaway_id
        guild_settings.next_giveaway_id += 1
        save_guild_settings(guild_settings) # Save incremented ID immediately


        # Parse bonus roles
        bonus_dict = {}
        if bonus_roles:
            pattern = re.compile(r"<@&(\d+)>:(\d+)")
            matches = pattern.findall(bonus_roles)
            for role_id_str, bonus_count_str in matches:
                try:
                    role_id = int(role_id_str)
                    bonus_count = int(bonus_count_str)
                    if bonus_count > 0:
                         if guild.get_role(role_id):
                             bonus_dict[role_id] = bonus_count
                         else:
                              logger.warning(f"Bonus role ID {role_id} not found in guild {guild.id} for giveaway start.")
                    else:
                        logger.warning(f"Ignoring non-positive bonus count {bonus_count} for role ID {role_id}")
                except ValueError:
                    logger.warning(f"Invalid format in bonus roles part: <@&{role_id_str}>:{bonus_count_str}")
            if not bonus_dict and bonus_roles.strip():
                 await interaction.followup.send("Warning: Could not parse any valid bonus roles. Format: `@RoleName:Entries` (e.g., `@VIP:2 @Booster:1`). Make sure roles exist.", ephemeral=True)

        # Parse bypass roles
        bypass_list = []
        if bypass_roles:
            pattern = re.compile(r"<@&(\d+)>")
            matches = pattern.findall(bypass_roles)
            for role_id_str in matches:
                try:
                    role_id = int(role_id_str)
                    if guild.get_role(role_id):
                        bypass_list.append(role_id)
                    else:
                        logger.warning(f"Bypass role ID {role_id} not found in guild {guild.id} for giveaway start.")
                except ValueError:
                     logger.warning(f"Invalid format in bypass roles part: <@&{role_id_str}>")
            if not bypass_list and bypass_roles.strip():
                await interaction.followup.send("Warning: Could not parse any valid bypass roles. Format: `@Role1 @Role2`. Make sure roles exist.", ephemeral=True)

        # Parse keywords
        keyword_list = []
        if keywords:
            keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
            if not keyword_list:
                 await interaction.followup.send("Warning: Could not parse any valid keywords from the input string.", ephemeral=True)


        # Create preliminary data (message ID comes after sending)
        temp_giveaway = GiveawayData(
            giveaway_id=sequential_id,
            message_id=0, # Placeholder
            channel_id=target_channel.id,
            guild_id=guild.id,
            prize=prize,
            host_id=interaction.user.id,
            winners_count=winners,
            start_time=start_time,
            end_time=end_time,
            required_role_id=required_role.id if required_role else None,
            bonus_entries=bonus_dict,
            bypass_role_ids=bypass_list,
            blacklist_role_id=blacklist_role.id if blacklist_role else None,
            min_messages=min_messages,
            message_count_channel_id=count_channel.id if count_channel else None,
            message_cooldown_seconds=cooldown_seconds,
            required_keywords=keyword_list,
            donor_id=donor.id if donor else None,
            image_url=image_url,
            participants={},
            ended=False,
            task_scheduled=False,
            is_drop=False # Explicitly set for standard giveaway
        )

        # Create embed (without message ID initially)
        try:
            # Pass guild settings to embed function
            guild_settings_for_embed = self.guild_settings.get(guild.id) # Fetch settings for embed
            embed = create_giveaway_embed(temp_giveaway, self.bot, status="active", guild_settings=guild_settings_for_embed)
            giveaway_msg = await target_channel.send(embed=embed, view=ActiveGiveawayView(self)) # Use ActiveGiveawayView
        except discord.Forbidden:
             await interaction.followup.send(f"I lack permissions to send messages or embeds in {target_channel.mention}.", ephemeral=True)
             # Revert sequential ID counter if message sending fails? Or accept the gap? Let's accept the gap for simplicity.
             return
        except Exception as e:
            logger.error(f"Failed to send giveaway message in {target_channel.id} for guild {guild.id}: {e}", exc_info=True)
            await interaction.followup.send("An error occurred while trying to post the giveaway.", ephemeral=True)
            return

        # Now update the giveaway data with the actual message ID
        temp_giveaway.message_id = giveaway_msg.id
        # Update the embed footer with the correct IDs
        embed.set_footer(text=guild_settings_for_embed.embed_footer.format(giveaway_id=temp_giveaway.giveaway_id)) # Use custom footer
        try:
            await giveaway_msg.edit(embed=embed)
        except Exception as e:
            logger.warning(f"Failed to update embed footer with IDs for giveaway message {giveaway_msg.id}: {e}")


        # Store and schedule
        self.active_giveaways[giveaway_msg.id] = temp_giveaway
        self._sequential_id_map[(temp_giveaway.guild_id, temp_giveaway.giveaway_id)] = temp_giveaway.message_id
        self.save_active_giveaways_for_guild(temp_giveaway.guild_id)
        self.schedule_giveaway_end(temp_giveaway) # Schedule the end task for standard giveaways

        # Increment host and donor stats (New)
        if guild.id not in self.user_stats:
             self.user_stats[guild.id] = load_guild_user_stats(guild.id)
        guild_stats = self.user_stats[guild.id]
        now = datetime.now(timezone.utc)

        # Increment host stats
        host_id = interaction.user.id
        if host_id not in guild_stats:
             guild_stats[host_id] = UserGiveawayStats(user_id=host_id, guild_id=guild.id)
        guild_stats[host_id].hosted_count += 1
        guild_stats[host_id].hosted_last_timestamp = now

        # Increment donor stats if donor exists
        if donor:
             donor_id = donor.id
             if donor_id not in guild_stats:
                  guild_stats[donor_id] = UserGiveawayStats(user_id=donor_id, guild_id=guild.id)
             guild_stats[donor_id].donated_count += 1
             guild_stats[donor_id].donated_last_timestamp = now

        save_guild_user_stats(guild_stats, guild.id)


        logger.info(f"Giveaway {temp_giveaway.giveaway_id}/{giveaway_msg.id} started by {interaction.user} in {target_channel.name} ({target_channel.id}) for guild {guild.id}.")
        await interaction.followup.send(
            f"✅ Giveaway **{temp_giveaway.giveaway_id}** for **{prize}** started in {target_channel.mention}! Ending <t:{int(end_time.timestamp())}:R>.",
            ephemeral=True
        )
        await self.log_giveaway_event("start", temp_giveaway, interaction.user) # Log start


    @g_group.command(name="drop", description="Start a drop giveaway (first to join wins instantly).")
    @app_commands.describe(
        prize="The prize for the drop.",
        channel="Channel to post the drop in (defaults to current).",
        image_url="URL of an image for the embed."
    )
    @app_commands.checks.has_permissions(manage_guild=True) # Default check, adjustable by staff role
    async def gdrop(self, interaction: discord.Interaction,
                    prize: str,
                    channel: Optional[discord.TextChannel] = None,
                    image_url: Optional[str] = None):
        """Starts a drop giveaway."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check using guild settings if available
        guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id))
        self.guild_settings[guild.id] = guild_settings
        member = guild.get_member(interaction.user.id)

        is_staff = False
        if guild_settings.staff_role_id and member:
             staff_role = guild.get_role(guild_settings.staff_role_id)
             if staff_role and staff_role in member.roles:
                  is_staff = True

        if not member.guild_permissions.manage_guild and not is_staff:
            await interaction.response.send_message("You need the 'Manage Guild' permission or the configured staff role to use this command.", ephemeral=True)
            return


        await interaction.response.defer(ephemeral=True, thinking=True)

        target_channel = channel or interaction.channel
        if not isinstance(target_channel, discord.TextChannel):
            await interaction.followup.send("Invalid channel selected.", ephemeral=True)
            return

        perms = target_channel.permissions_for(guild.me)
        if not perms.send_messages or not perms.embed_links:
            await interaction.followup.send(f"I need 'Send Messages' and 'Embed Links' permissions in {target_channel.mention}.", ephemeral=True)
            return

        start_time = datetime.now(timezone.utc)
        # Drops end instantly when the first person joins, but we need an end_time
        # for the data structure and embed timestamp. Set a short arbitrary duration.
        end_time = start_time + timedelta(minutes=5) # Arbitrary end time for display/storage


        sequential_id = guild_settings.next_giveaway_id
        guild_settings.next_giveaway_id += 1
        save_guild_settings(guild_settings)

        # Create giveaway data for a drop
        temp_giveaway = GiveawayData(
            giveaway_id=sequential_id,
            message_id=0, # Placeholder
            channel_id=target_channel.id,
            guild_id=guild.id,
            prize=prize,
            host_id=interaction.user.id,
            winners_count=1, # Always 1 winner for drops
            start_time=start_time,
            end_time=end_time, # Use arbitrary end time
            required_role_id=None, # No requirements for drops
            bonus_entries={}, # No bonus entries for drops
            bypass_role_ids=[], # No bypass needed if no requirements
            blacklist_role_id=None, # Blacklist still applies
            min_messages=0, # No message requirements
            message_count_channel_id=None,
            message_cooldown_seconds=0,
            required_keywords=[],
            donor_id=None, # No donor option for drops? Or add it? Let's add it later if needed.
            image_url=image_url,
            participants={}, # Empty initially
            ended=False,
            task_scheduled=False, # Drops are ended by the join logic, not a timer task
            is_drop=True # Mark as a drop
        )

        # Create embed using drop settings
        guild_settings_for_embed = self.guild_settings.get(guild.id)
        embed = create_giveaway_embed(temp_giveaway, self.bot, status="active", guild_settings=guild_settings_for_embed)

        try:
            drop_msg = await target_channel.send(embed=embed, view=ActiveGiveawayView(self)) # Use ActiveGiveawayView
        except discord.Forbidden:
             await interaction.followup.send(f"I lack permissions to send messages or embeds in {target_channel.mention}.", ephemeral=True)
             return
        except Exception as e:
            logger.error(f"Failed to send drop message in {target_channel.id} for guild {guild.id}: {e}", exc_info=True)
            await interaction.followup.send("An error occurred while trying to post the drop.", ephemeral=True)
            return

        # Now update the giveaway data with the actual message ID
        temp_giveaway.message_id = drop_msg.id
        # Update the embed footer with the correct IDs
        embed.set_footer(text=guild_settings_for_embed.embed_footer.format(giveaway_id=temp_giveaway.giveaway_id)) # Use custom footer
        try:
            await drop_msg.edit(embed=embed)
        except Exception as e:
            logger.warning(f"Failed to update embed footer for drop message {drop_msg.id}: {e}")


        # Store the drop giveaway
        self.active_giveaways[drop_msg.id] = temp_giveaway
        self._sequential_id_map[(temp_giveaway.guild_id, temp_giveaway.giveaway_id)] = temp_giveaway.message_id
        self.save_active_giveaways_for_guild(temp_giveaway.guild_id)
        # No schedule_giveaway_end for drops

        # Increment host stats (New)
        if guild.id not in self.user_stats:
             self.user_stats[guild.id] = load_guild_user_stats(guild.id)
        guild_stats = self.user_stats[guild.id]
        now = datetime.now(timezone.utc)

        host_id = interaction.user.id
        if host_id not in guild_stats:
             guild_stats[host_id] = UserGiveawayStats(user_id=host_id, guild_id=guild.id)
        guild_stats[host_id].hosted_count += 1
        guild_stats[host_id].hosted_last_timestamp = now
        save_guild_user_stats(guild_stats, guild.id)


        logger.info(f"Drop Giveaway {temp_giveaway.giveaway_id}/{drop_msg.id} started by {interaction.user} in {target_channel.name} ({target_channel.id}) for guild {guild.id}.")
        await interaction.followup.send(
            f"✅ Drop Giveaway **{temp_giveaway.giveaway_id}** for **{prize}** started in {target_channel.mention}! Be the first to join to win!",
            ephemeral=True
        )
        await self.log_giveaway_event("start", temp_giveaway, interaction.user) # Log start


    @g_group.command(name="profile", description="Show giveaway statistics for a user.")
    @app_commands.describe(user="The user to get stats for (defaults to you).")
    async def gprofile_command(self, interaction: discord.Interaction, user: Optional[discord.User] = None):
        """Shows giveaway statistics for a user in this server."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # No explicit permission check needed, anyone can view stats.
        await interaction.response.defer(thinking=True)

        target_user = user or interaction.user # Default to self

        # Load stats for the guild
        if guild.id not in self.user_stats:
             self.user_stats[guild.id] = load_guild_user_stats(guild.id)
        guild_stats = self.user_stats[guild.id]

        user_stats = guild_stats.get(target_user.id)

        embed = discord.Embed(
            title=f"Giveaway Statistics for {target_user.display_name}",
            color=discord.Color.from_rgb(*tuple(int("20010c"[i:i+2], 16) for i in (0, 2, 4))), # Use provided color
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=f"{self.bot.user.name}・Giveaway Statistics", icon_url=self.bot.user.avatar.url if self.bot.user.avatar else None) # Use bot's name/avatar


        if not user_stats:
            embed.description = f"No giveaway statistics found for {target_user.mention} in this server."
        else:
            # Calculate time-based stats based on last event timestamp
            now = datetime.now(timezone.utc)
            one_hour_ago = now - timedelta(hours=1)
            twenty_four_hours_ago = now - timedelta(hours=24)
            one_week_ago = now - timedelta(weeks=1)

            hosted_last_hour = 'Yes' if user_stats.hosted_last_timestamp and user_stats.hosted_last_timestamp > one_hour_ago else 'No'
            hosted_last_24h = 'Yes' if user_stats.hosted_last_timestamp and user_stats.hosted_last_timestamp > twenty_four_hours_ago else 'No'
            hosted_last_week = 'Yes' if user_stats.hosted_last_timestamp and user_stats.hosted_last_timestamp > one_week_ago else 'No'

            donated_last_hour = 'Yes' if user_stats.donated_last_timestamp and user_stats.donated_last_timestamp > one_hour_ago else 'No'
            donated_last_24h = 'Yes' if user_stats.donated_last_timestamp and user_stats.donated_last_timestamp > twenty_four_hours_ago else 'No'
            donated_last_week = 'Yes' if user_stats.donated_last_timestamp and user_stats.donated_last_timestamp > one_week_ago else 'No'

            won_last_hour = 'Yes' if user_stats.won_last_timestamp and user_stats.won_last_timestamp > one_hour_ago else 'No'
            won_last_24h = 'Yes' if user_stats.won_last_timestamp and user_stats.won_last_timestamp > twenty_four_hours_ago else 'No'
            won_last_week = 'Yes' if user_stats.won_last_timestamp and user_stats.won_last_timestamp > one_week_ago else 'No'


            # Use the template structure with our limited data interpretation
            description_text = (
                f"🎉 **Giveaways Hosted**\n"
                f"Last hour: `{hosted_last_hour}`\n"
                f"Last 24H: `{hosted_last_24h}`\n"
                f"Last week: `{hosted_last_week}`\n"
                f"Total: `{user_stats.hosted_count}`\n" # Add total count
                f"\n" # Add a separator for clarity
                f"🎁 **Giveaways Donated**\n"
                f"Last hour: `{donated_last_hour}`\n"
                f"Last 24H: `{donated_last_24h}`\n"
                f"Last week: `{donated_last_week}`\n"
                f"Total: `{user_stats.donated_count}`\n" # Add total count
                f"\n" # Add a separator
                f"🏆 **Giveaways Won**\n"
                f"Last hour: `{won_last_hour}`\n"
                f"Last 24H: `{won_last_24h}`\n"
                f"Last week: `{won_last_week}`\n"
                f"Total: `{user_stats.won_count}`" # Add total count
            )
            embed.description = description_text

        embed.set_footer(text="Showing statistics for this server.\nNote: Timeframe checks indicate if the *last* event occurred within that period.")


        await interaction.followup.send(embed=embed)


    @g_group.command(name="list", description="List active giveaways in this server.")
    # Use the staff role check if configured, otherwise require manage_messages (less strict than manage_guild)
    @app_commands.checks.has_permissions(manage_messages=True)
    async def glist_command(self, interaction: discord.Interaction):
        """Lists all currently active giveaways managed by the bot in this guild."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check using guild settings if available
        guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id))
        self.guild_settings[guild.id] = guild_settings
        member = guild.get_member(interaction.user.id)

        is_staff = False
        if guild_settings.staff_role_id and member:
             staff_role = guild.get_role(guild_settings.staff_role_id)
             if staff_role and staff_role in member.roles:
                  is_staff = True

        # Allow if they have manage_messages OR are staff
        if not member.guild_permissions.manage_messages and not is_staff:
             await interaction.response.send_message("You need the 'Manage Messages' permission or the configured staff role to use this command.", ephemeral=True)
             return


        await interaction.response.defer(ephemeral=True, thinking=True)
        guild_giveaways = [gw for gw in self.active_giveaways.values() if gw.guild_id == guild.id and not gw.ended]

        if not guild_giveaways:
            await interaction.followup.send("There are no active giveaways in this server right now.", ephemeral=True)
            return

        # Sort by sequential ID for better readability
        guild_giveaways.sort(key=lambda gw: gw.giveaway_id)

        embed = discord.Embed(title=f"Active Giveaways in {guild.name}", color=discord.Color.green())
        embed.timestamp = datetime.now(timezone.utc)

        for gw in guild_giveaways:
            channel = guild.get_channel(gw.channel_id)
            channel_mention = channel.mention if channel else f"ID: {gw.channel_id}"
            host = guild.get_member(gw.host_id) # Use member for potential display name
            host_mention = host.mention if host else f"ID: {gw.host_id}"
            time_left = f"<t:{int(gw.end_time.timestamp())}:R>"
            participant_count = len(gw.participants)
            try:
                 msg_link = f"https://discord.com/channels/{gw.guild_id}/{gw.channel_id}/{gw.message_id}"
            except:
                 msg_link = "(Link unavailable)"

            giveaway_type = "Drop" if gw.is_drop else "Standard"

            field_value = (
                f"**Prize:** {gw.prize}\n"
                f"**Type:** {giveaway_type}\n" # Add type
                f"**Channel:** {channel_mention}\n"
                f"**Host:** {host_mention}\n"
                f"**Ends:** {time_left} (<t:{int(gw.end_time.timestamp())}:F>)\n"
                f"**Winners:** {gw.winners_count}\n"
                f"**Participants:** {participant_count}\n"
                f"[Jump to Message]({msg_link})"
            )
            embed.add_field(name=f"ID: {gw.giveaway_id}", value=field_value, inline=False)

            if len(embed) > 5900: # Check embed limits (max 6000)
                 embed.description = "Too many giveaways to list all details..."
                 break # Stop adding fields if near limit

        await interaction.followup.send(embed=embed, ephemeral=True)


    @g_group.command(name="cancel", description="Cancel an active giveaway (no winners drawn).")
    @app_commands.describe(giveaway_id="The Sequential Giveaway ID to cancel.")
    @app_commands.checks.has_permissions(manage_guild=True) # Default check
    async def gcancel_command(self, interaction: discord.Interaction, giveaway_id: int):
        """Cancels an active giveaway."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check
        guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id))
        self.guild_settings[guild.id] = guild_settings
        member = guild.get_member(interaction.user.id)
        is_staff = False
        if guild_settings.staff_role_id and member:
             staff_role = guild.get_role(guild_settings.staff_role_id)
             if staff_role and staff_role in member.roles:
                  is_staff = True
        if not member.guild_permissions.manage_guild and not is_staff:
            await interaction.response.send_message("You need the 'Manage Guild' permission or the configured staff role to use this command.", ephemeral=True)
            return


        await interaction.response.defer(ephemeral=True, thinking=True)

        # Lookup giveaway by sequential ID
        giveaway = self.get_giveaway_by_sequential_id(guild.id, giveaway_id)

        if not giveaway or giveaway.ended or giveaway.guild_id != guild.id:
            await interaction.followup.send(f"No active giveaway found with ID {giveaway_id} in this server.", ephemeral=True)
            return

        logger.info(f"Cancelling giveaway {giveaway_id}/{giveaway.message_id} by request of {interaction.user} in guild {guild.id}.")

        # Cancel the end task (only applicable to standard giveaways)
        if not giveaway.is_drop:
             task = self.giveaway_end_tasks.pop(giveaway.message_id, None)
             if task:
                 task.cancel()

        giveaway.ended = True # Mark as ended (cancelled)
        giveaway.task_scheduled = False # Task is no longer relevant


        # Remove from active, save state for this guild
        self.active_giveaways.pop(giveaway.message_id, None)
        self.save_active_giveaways_for_guild(giveaway.guild_id)
        # Optionally add to ended cache marked as cancelled? For now, just remove from active.
        # Also remove from sequential ID map? No, keep it for historical lookup if needed.

        # Update the message embed
        channel = self.bot.get_channel(giveaway.channel_id)
        if channel:
            try:
                original_msg = await channel.fetch_message(giveaway.message_id)
                cancel_embed = create_giveaway_embed(giveaway, self.bot, status="cancelled", guild_settings=guild_settings)
                # Replace the view with EndedGiveawayView (buttons should be disabled by logic)
                ended_view = EndedGiveawayView(self, giveaway=giveaway) # Create instance
                await original_msg.edit(embed=cancel_embed, view=ended_view) # Replace the view

                await interaction.followup.send(f"✅ Giveaway **{giveaway_id}** (Prize: {giveaway.prize}) has been cancelled.", ephemeral=True)
                await self.log_giveaway_event("cancel", giveaway, interaction.user)

            except discord.NotFound:
                 await interaction.followup.send(f"✅ Giveaway **{giveaway_id}** cancelled, but couldn't find the original message to update.", ephemeral=True)
                 await self.log_giveaway_event("cancel", giveaway, interaction.user) # Log even if message update fails
            except discord.Forbidden:
                 await interaction.followup.send(f"✅ Giveaway **{giveaway_id}** cancelled, but I lack permission to edit the original message.", ephemeral=True)
                 await self.log_giveaway_event("cancel", giveaway, interaction.user) # Log even if message update fails
            except Exception as e:
                 logger.error(f"Error updating cancelled giveaway message {giveaway.message_id}: {e}", exc_info=True)
                 await interaction.followup.send(f"✅ Giveaway **{giveaway_id}** cancelled, but an error occurred updating the message.", ephemeral=True)
                 await self.log_giveaway_event("cancel", giveaway, interaction.user) # Log even if message update fails
        else:
            logger.warning(f"Could not find channel {giveaway.channel_id} to update cancelled giveaway {giveaway_id}/{giveaway.message_id}.")
            await interaction.followup.send(f"✅ Giveaway **{giveaway_id}** cancelled (channel not found).", ephemeral=True)
            await self.log_giveaway_event("cancel", giveaway, interaction.user) # Log even if channel not found


    @g_group.command(name="end", description="End an active giveaway immediately and draw winners.")
    @app_commands.describe(giveaway_id="The Sequential Giveaway ID to end.")
    @app_commands.checks.has_permissions(manage_guild=True) # Default check
    async def gend_command(self, interaction: discord.Interaction, giveaway_id: int):
        """Manually ends a giveaway now."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check
        guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id))
        self.guild_settings[guild.id] = guild_settings
        member = guild.get_member(interaction.user.id)
        is_staff = False
        if guild_settings.staff_role_id and member:
             staff_role = guild.get_role(guild_settings.staff_role_id)
             if staff_role and staff_role in member.roles:
                  is_staff = True
        if not member.guild_permissions.manage_guild and not is_staff:
            await interaction.response.send_message("You need the 'Manage Guild' permission or the configured staff role to use this command.", ephemeral=True)
            return


        await interaction.response.defer(ephemeral=True, thinking=True)

        # Lookup giveaway by sequential ID
        giveaway = self.get_giveaway_by_sequential_id(guild.id, giveaway_id)

        if not giveaway or giveaway.ended or giveaway.guild_id != guild.id:
            await interaction.followup.send(f"No active giveaway found with ID {giveaway_id} in this server.", ephemeral=True)
            return

        # Cancel the scheduled end task (if exists and is standard giveaway)
        if not giveaway.is_drop:
             task = self.giveaway_end_tasks.pop(giveaway.message_id, None)
             if task:
                  task.cancel()
                  logger.info(f"Cancelled scheduled end task for giveaway {giveaway_id}/{giveaway.message_id} due to manual end.")

        # Trigger the end logic
        await self.end_giveaway(giveaway.message_id, ended_by=interaction.user)
        await interaction.followup.send(f"Giveaway **{giveaway_id}** (Prize: {giveaway.prize}) ended.", ephemeral=True)


    @g_group.command(name="reroll", description="Reroll winners for a recently ended giveaway.")
    @app_commands.describe(giveaway_id="The Sequential Giveaway ID of the ended giveaway to reroll.")
    @app_commands.checks.has_permissions(manage_guild=True) # Default check
    async def greroll_command(self, interaction: discord.Interaction, giveaway_id: int):
        """Rerolls winners for an ended giveaway."""
        guild = interaction.guild
        if not guild:
             await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
             return

        # Permissions check
        guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id))
        self.guild_settings[guild.id] = guild_settings
        member = guild.get_member(interaction.user.id)
        is_staff = False
        if guild_settings.staff_role_id and member:
             staff_role = guild.get_role(guild_settings.staff_role_id)
             if staff_role and staff_role in member.roles:
                  is_staff = True
        if not member.guild_permissions.manage_guild and not is_staff:
            await interaction.response.send_message("You need the 'Manage Guild' permission or the configured staff role to use this command.", ephemeral=True)
            return


        await interaction.response.defer(ephemeral=True, thinking=True)

        # Lookup giveaway by sequential ID (will check active and ended cache)
        giveaway = self.get_giveaway_by_sequential_id(guild.id, giveaway_id)

        if not giveaway or giveaway.guild_id != guild.id:
            await interaction.followup.send(f"Could not find data for giveaway ID **{giveaway_id}** in this server. It might be too old or never existed.", ephemeral=True)
            return

        # Ensure it actually ended
        if not giveaway.ended:
            await interaction.followup.send(f"Giveaway ID **{giveaway_id}** has not officially ended yet.", ephemeral=True)
            return

        logger.info(f"Rerolling giveaway {giveaway_id}/{giveaway.message_id} by request of {interaction.user} in guild {guild.id}.")

        # Trigger the reroll logic (call the new function)
        await self.perform_reroll(interaction, giveaway)

        # The perform_reroll function handles the followup and logging

    @g_group.command(name="settings", description="Configure giveaway settings for this server.")
    @app_commands.describe(
         staff_role="Role that can manage giveaways (overrides default permissions). Select 'Unset' to clear.", # Add unset instruction
         default_blacklist="Default role whose members are blacklisted. Select 'Unset' to clear.", # Add unset instruction
         default_bypass="Default roles that bypass requirements (mention roles, separated by space). Type 'none' to clear.", # Add unset instruction
         log_channel="Channel to send detailed giveaway logs. Select 'Unset' to clear.", # Add unset instruction
         # --- New Arguments for Customization ---
         embed_colour="Embed color (hex e.g., #3498db).",
         embed_winners_colour="Embed color when winners drawn (hex).",
         embed_nowinners_colour="Embed color when no winners (hex).",
         embed_cancelled_colour="Embed color when cancelled (hex).",
         embed_description="Text in the giveaway embed (use {prize}, {winners}, {host}).",
         embed_drop_description="Text in drop giveaway embed (use {prize}, {winners}, {host}).",
         embed_header="Header text above giveaway embed (use {prize}, {winners}).",
         embed_header_end="Header text above ended embed (use {prize}, {winners}).",
         embed_footer="Footer text (use {giveaway_id}). Custom emoji won't work.",
         win_message="Message sent to channel when winners drawn ({winners}, {prize}).",
         nowinners_message="Message sent when no winners ({prize}).",
         reroll_message="Message sent when rerolled ({winners}, {prize}).",
         dm_winner="Send win DM to winners? (True/False)", # Use a boolean choice or string
         title_dm_hostembed="Host DM embed title ({prize}, {guild_name}). Custom emoji won't work.",
         colour_dm_hostembed="Host DM embed color (hex or 'random').",
         description_dm_hostembed="Host DM embed desc ({prize}, {guild_name}). Markdown supported.",
         thumbnail_dm_hostembed="Host DM embed thumbnail URL (.png/.gif).",
         footer_dm_hostembed="Host DM embed footer ({giveaway_id}). Custom emoji won't work.",
         title_dm_winembed="Winner DM embed title ({prize}, {guild_name}). Custom emoji won't work.",
         colour_dm_winembed="Winner DM embed color (hex or 'random').",
         description_dm_winembed="Winner DM embed desc ({prize}, {guild_name}). Markdown supported.",
         thumbnail_dm_winembed="Winner DM embed thumbnail URL (.png/.gif).",
         footer_dm_winembed="Winner DM embed footer ({giveaway_id}). Custom emoji won't work.",
    )
    # Add choices for dm_winner boolean
    @app_commands.choices(dm_winner=[
        app_commands.Choice(name="True", value="True"),
        app_commands.Choice(name="False", value="False"),
    ])
    @app_commands.checks.has_permissions(manage_guild=True) # Only guild managers can change settings
    @app_commands.autocomplete("staff_role", role_autocomplete)
    @app_commands.autocomplete("default_blacklist", role_autocomplete)
    @app_commands.autocomplete("log_channel", channel_autocomplete)
    async def gsettings_command(self, interaction: discord.Interaction,
                                staff_role: Optional[discord.Role] = None,
                                default_blacklist: Optional[discord.Role] = None,
                                default_bypass: Optional[str] = None,
                                log_channel: Optional[discord.TextChannel] = None,
                                # --- New Parameters ---
                                embed_colour: Optional[str] = None,
                                embed_winners_colour: Optional[str] = None,
                                embed_nowinners_colour: Optional[str] = None,
                                embed_cancelled_colour: Optional[str] = None,
                                embed_description: Optional[str] = None,
                                embed_drop_description: Optional[str] = None,
                                embed_header: Optional[str] = None,
                                embed_header_end: Optional[str] = None,
                                embed_footer: Optional[str] = None,
                                win_message: Optional[str] = None,
                                nowinners_message: Optional[str] = None,
                                reroll_message: Optional[str] = None,
                                dm_winner: Optional[str] = None, # Changed to string to use choices
                                title_dm_hostembed: Optional[str] = None,
                                colour_dm_hostembed: Optional[str] = None,
                                description_dm_hostembed: Optional[str] = None,
                                thumbnail_dm_hostembed: Optional[str] = None,
                                footer_dm_hostembed: Optional[str] = None,
                                title_dm_winembed: Optional[str] = None,
                                colour_dm_winembed: Optional[str] = None,
                                description_dm_winembed: Optional[str] = None,
                                thumbnail_dm_winembed: Optional[str] = None,
                                footer_dm_winembed: Optional[str] = None
                                ):
         """Sets server-specific giveaway configuration."""
         guild = interaction.guild
         if not guild:
              await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
              return

         await interaction.response.defer(ephemeral=True, thinking=True)

         # Load current settings or get default
         guild_settings = self.guild_settings.get(guild.id, load_guild_settings(guild.id))

         # Track changes to provide feedback
         changes = []
         warnings = []


         # Update settings based on provided arguments
         # Handle unset using special values (e.g., 0 for roles/channels, 'none' for strings, 'False' for boolean)
         if staff_role is not None:
             if staff_role.id == 0: # Check if the '0' role was selected to unset
                  guild_settings.staff_role_id = None
                  changes.append("Staff role unset.")
             else:
                 guild_settings.staff_role_id = staff_role.id
                 changes.append(f"Staff role set to {staff_role.mention}.")

         if default_blacklist is not None:
             if default_blacklist.id == 0:
                  guild_settings.default_blacklist_role_id = None
                  changes.append("Default blacklist role unset.")
             else:
                 guild_settings.default_blacklist_role_id = default_blacklist.id
                 changes.append(f"Default blacklist role set to {default_blacklist.mention}.")

         if default_bypass is not None:
             if default_bypass.lower() == 'none': # Use 'none' as a string indicator to clear
                  guild_settings.default_bypass_role_ids = []
                  changes.append("Default bypass roles cleared.")
             else:
                # Parse default bypass roles string
                default_bypass_list = []
                pattern = re.compile(r"<@&(\d+)>")
                matches = pattern.findall(default_bypass)
                parsed_roles_count = 0
                for role_id_str in matches:
                    try:
                        role_id = int(role_id_str)
                        if guild.get_role(role_id):
                            default_bypass_list.append(role_id)
                            parsed_roles_count += 1
                        else:
                            logger.warning(f"Default bypass role ID {role_id} not found in guild {guild.id} during settings update.")
                    except ValueError:
                         logger.warning(f"Invalid format in default bypass roles part: <@&{role_id_str}>")

                guild_settings.default_bypass_role_ids = default_bypass_list
                if default_bypass_list:
                    bypass_mentions = [guild.get_role(rid).mention for rid in default_bypass_list if guild.get_role(rid)]
                    changes.append(f"Default bypass roles set to: {', '.join(bypass_mentions) or 'None'}.")
                else:
                     if default_bypass.strip() != '':
                         warnings.append("Could not parse any valid default bypass roles. Format: `@Role1 @Role2`. Make sure roles exist. Default bypass roles cleared.")
                     else:
                          changes.append("Default bypass roles cleared.")


         if log_channel is not None:
             if log_channel.id == 0: # Check if the '0' channel was selected to unset
                  guild_settings.log_channel_id = None
                  changes.append("Log channel unset.")
             else:
                 # Check bot's permissions in the log channel
                 bot_member = guild.get_member(self.bot.user.id)
                 if not bot_member or not log_channel.permissions_for(bot_member).send_messages or not log_channel.permissions_for(bot_member).embed_links:
                      warnings.append(f"I need 'Send Messages' and 'Embed Links' permissions in {log_channel.mention} to send logs. Log channel setting not saved.")
                 else:
                     guild_settings.log_channel_id = log_channel.id
                     changes.append(f"Giveaway log channel set to {log_channel.mention}.")


         # --- Update New Settings ---
         if embed_colour is not None:
             # Add validation for hex color format
             if re.match(r"^#([A-Fa-f0-9]{6})$", embed_colour):
                 guild_settings.embed_colour = embed_colour
                 changes.append(f"Embed color set to {embed_colour}.")
             else:
                 warnings.append(f"Invalid hex color format for embed_colour: `{embed_colour}`. Please use #RRGGBB. Setting not saved.")

         if embed_winners_colour is not None:
              if re.match(r"^#([A-Fa-f0-9]{6})$", embed_winners_colour):
                  guild_settings.embed_winners_colour = embed_winners_colour
                  changes.append(f"Winners embed color set to {embed_winners_colour}.")
              else:
                  warnings.append(f"Invalid hex color format for embed_winners_colour: `{embed_winners_colour}`. Setting not saved.")

         if embed_nowinners_colour is not None:
              if re.match(r"^#([A-Fa-f0-9]{6})$", embed_nowinners_colour):
                  guild_settings.embed_nowinners_colour = embed_nowinners_colour
                  changes.append(f"No winners embed color set to {embed_nowinners_colour}.")
              else:
                  warnings.append(f"Invalid hex color format for embed_nowinners_colour: `{embed_nowinners_colour}`. Setting not saved.")

         if embed_cancelled_colour is not None:
              if re.match(r"^#([A-Fa-f0-9]{6})$", embed_cancelled_colour):
                  guild_settings.embed_cancelled_colour = embed_cancelled_colour
                  changes.append(f"Cancelled embed color set to {embed_cancelled_colour}.")
              else:
                  warnings.append(f"Invalid hex color format for embed_cancelled_colour: `{embed_cancelled_colour}`. Setting not saved.")


         if embed_description is not None:
             guild_settings.embed_description = embed_description
             changes.append("Giveaway embed description updated.")

         if embed_drop_description is not None:
             guild_settings.embed_drop_description = embed_drop_description
             changes.append("Drop embed description updated.")

         if embed_header is not None:
             guild_settings.embed_header = embed_header
             changes.append("Giveaway embed header updated.")

         if embed_header_end is not None:
             guild_settings.embed_header_end = embed_header_end
             changes.append("Ended embed header updated.")

         if embed_footer is not None:
             # Remove any custom emoji '<:name:id>' from footer string before saving
             cleaned_footer = re.sub(r"<:\w+:\d+>", "", embed_footer)
             guild_settings.embed_footer = cleaned_footer
             changes.append("Embed footer updated (custom emojis removed).")


         if win_message is not None:
             guild_settings.win_message = win_message
             changes.append("Winner announcement message updated.")

         if nowinners_message is not None:
             guild_settings.nowinners_message = nowinners_message
             changes.append("No winners message updated.")

         if reroll_message is not None:
             guild_settings.reroll_message = reroll_message
             changes.append("Reroll announcement message updated.")


         if dm_winner is not None:
             # Parse the string choice
             new_dm_winner_setting = dm_winner == "True"
             guild_settings.dm_winner = new_dm_winner_setting
             changes.append(f"DM winner setting set to {guild_settings.dm_winner}.")


         if title_dm_hostembed is not None:
             # Remove any custom emoji '<:name:id>' from title string before saving
             cleaned_title = re.sub(r"<:\w+:\d+>", "", title_dm_hostembed)
             guild_settings.title_dm_hostembed = cleaned_title
             changes.append("Host DM embed title updated (custom emojis removed).")

         if colour_dm_hostembed is not None:
             # Add validation for hex color or 'random'
             if re.match(r"^#([A-Fa-f0-9]{6})$", colour_dm_hostembed) or colour_dm_hostembed.lower() == 'random':
                 guild_settings.colour_dm_hostembed = colour_dm_hostembed
                 changes.append(f"Host DM embed color set to {colour_dm_hostembed}.")
             else:
                 warnings.append(f"Invalid hex color or 'random' format for colour_dm_hostembed: `{colour_dm_hostembed}`. Setting not saved.")


         if description_dm_hostembed is not None:
             guild_settings.description_dm_hostembed = description_dm_hostembed
             changes.append("Host DM embed description updated.")

         if thumbnail_dm_hostembed is not None:
              # Add basic URL format validation
              if re.match(r"https?://.*\.(png|gif|jpg|jpeg|webp)\b", thumbnail_dm_hostembed, re.IGNORECASE): # Added more image extensions
                  guild_settings.thumbnail_dm_hostembed = thumbnail_dm_hostembed
                  changes.append(f"Host DM embed thumbnail set to {thumbnail_dm_hostembed}.")
              else:
                  warnings.append(f"Invalid URL format or file type for thumbnail_dm_hostembed: `{thumbnail_dm_hostembed}`. Must be a direct image URL (.png, .gif, .jpg, .jpeg, .webp). Setting not saved.")


         if footer_dm_hostembed is not None:
             # Remove any custom emoji '<:name:id>' from footer string before saving
             cleaned_footer = re.sub(r"<:\w+:\d+>", "", footer_dm_hostembed)
             guild_settings.footer_dm_hostembed = cleaned_footer
             changes.append("Host DM embed footer updated (custom emojis removed).")


         if title_dm_winembed is not None:
             # Remove any custom emoji '<:name:id>' from title string before saving
             cleaned_title = re.sub(r"<:\w+:\d+>", "", title_dm_winembed)
             guild_settings.title_dm_winembed = cleaned_title
             changes.append("Winner DM embed title updated (custom emojis removed).")

         if colour_dm_winembed is not None:
              # Add validation for hex color or 'random'
              if re.match(r"^#([A-Fa-f0-9]{6})$", colour_dm_winembed) or colour_dm_winembed.lower() == 'random':
                  guild_settings.colour_dm_winembed = colour_dm_winembed
                  changes.append(f"Winner DM embed color set to {colour_dm_winembed}.")
              else:
                  warnings.append(f"Invalid hex color or 'random' format for colour_dm_winembed: `{colour_dm_winembed}`. Setting not saved.")

         if description_dm_winembed is not None:
             guild_settings.description_dm_winembed = description_dm_winembed
             changes.append("Winner DM embed description updated.")

         if thumbnail_dm_winembed is not None:
              # Add basic URL format validation
              if re.match(r"https?://.*\.(png|gif|jpg|jpeg|webp)\b", thumbnail_dm_winembed, re.IGNORECASE):
                  guild_settings.thumbnail_dm_winembed = thumbnail_dm_winembed
                  changes.append(f"Winner DM embed thumbnail set to {thumbnail_dm_winembed}.")
              else:
                  warnings.append(f"Invalid URL format or file type for thumbnail_dm_winembed: `{thumbnail_dm_winembed}`. Must be a direct image URL (.png, .gif, .jpg, .jpeg, .webp). Setting not saved.")

         if footer_dm_winembed is not None:
             # Remove any custom emoji '<:name:id>' from footer string before saving
             cleaned_footer = re.sub(r"<:\w+:\d+>", "", footer_dm_winembed)
             guild_settings.footer_dm_winembed = cleaned_footer
             changes.append("Winner DM embed footer updated (custom emojis removed).")


         # If no arguments were provided, just show current settings
         if not changes and not warnings: # Check if any settings were successfully updated or had warnings
              embed = discord.Embed(title=f"Current Giveaway Settings for {guild.name}", color=discord.Color.blue())
              settings = guild_settings # Use the potentially updated settings to show

              staff_role_mention = guild.get_role(settings.staff_role_id).mention if settings.staff_role_id and guild.get_role(settings.staff_role_id) else "Not set"
              blacklist_role_mention = guild.get_role(settings.default_blacklist_role_id).mention if settings.default_blacklist_role_id and guild.get_role(settings.default_blacklist_role_id) else "Not set"
              bypass_role_mentions = [guild.get_role(rid).mention for rid in settings.default_bypass_role_ids if guild.get_role(rid)]
              bypass_roles_str = ", ".join(bypass_role_mentions) or "None set"
              log_channel_mention = guild.get_channel(settings.log_channel_id).mention if settings.log_channel_id and guild.get_channel(settings.log_channel_id) else "Not set"

              embed.add_field(name="Basic Settings", value=
                  f"Staff Role: {staff_role_mention}\n"
                  f"Default Blacklist Role: {blacklist_role_mention}\n"
                  f"Default Bypass Roles: {bypass_roles_str}\n"
                  f"Log Channel: {log_channel_mention}\n"
                  f"DM Winner: {settings.dm_winner}", inline=False)

              embed.add_field(name="Embed Appearance (Giveaway)", value=
                  f"Color: `{settings.embed_colour}`\n"
                  f"Winners Color: `{settings.embed_winners_colour}`\n"
                  f"No Winners Color: `{settings.embed_nowinners_colour}`\n"
                  f"Cancelled Color: `{settings.embed_cancelled_colour}`\n"
                  f"Header: `{settings.embed_header}`\n"
                  f"Header (Ended): `{settings.embed_header_end}`\n"
                  f"Footer: `{settings.embed_footer}`", inline=False)

              embed.add_field(name="Description Text", value=
                   f"Giveaway: `{settings.embed_description}`\n"
                   f"Drop: `{settings.embed_drop_description}`", inline=False)


              embed.add_field(name="Channel Messages", value=
                  f"Win Message: `{settings.win_message}`\n"
                  f"No Winners Message: `{settings.nowinners_message}`\n"
                  f"Reroll Message: `{settings.reroll_message}`", inline=False)


              embed.add_field(name="Host DM Embed", value=
                   f"Title: `{settings.title_dm_hostembed}`\n"
                   f"Color: `{settings.colour_dm_hostembed}`\n"
                   f"Thumbnail: `{settings.thumbnail_dm_hostembed or 'None'}`\n"
                   f"Footer: `{settings.footer_dm_hostembed}`", inline=False)
              # Description for host DM is likely long, maybe skip showing it here or truncate

              embed.add_field(name="Winner DM Embed", value=
                   f"Title: `{settings.title_dm_winembed}`\n"
                   f"Color: `{settings.colour_dm_winembed}`\n"
                   f"Thumbnail: `{settings.thumbnail_dm_winembed or 'None'}`\n"
                   f"Footer: `{settings.footer_dm_winembed}`", inline=False)
              # Description for winner DM is likely long, maybe skip showing it here or truncate

              embed.set_footer(text="Use the command with arguments to set these values.")
              await interaction.followup.send(embed=embed, ephemeral=True)
              return # Exit if just showing settings


         # Save updated settings if changes were made (even if there were warnings)
         self.guild_settings[guild.id] = guild_settings # Ensure cached
         save_guild_settings(guild_settings)

         feedback_message = ""
         if changes:
              feedback_message += "Giveaway settings updated:\n" + "\n".join(changes)
         if warnings:
              if feedback_message: feedback_message += "\n\n"
              feedback_message += "Warnings:\n" + "\n".join(warnings)

         if not feedback_message:
              feedback_message = "No valid settings were provided or changed."

         await interaction.followup.send(feedback_message, ephemeral=True)


    @g_group.command(name="help", description="Shows help information for giveaway commands.")
    async def ghelp_command(self, interaction: discord.Interaction):
         embed = discord.Embed(title="🎁 Giveaway Bot Help", color=discord.Color.purple())
         embed.description = "Manage giveaways and drops using these slash commands:"

         embed.add_field(name="/g start", value="Starts a new standard giveaway.\n*Args: `duration`, `winners`, `prize`, `[channel]`, `[required_role]`, `[bonus_roles]`, `[bypass_roles]`, `[blacklist_role]`, `[min_messages]`, `[message_channel]`, `[message_cooldown]`, `[keywords]`, `[donor]`, `[image_url]`*", inline=False)
         embed.add_field(name="/g drop", value="Starts a drop giveaway (first to join wins).\n*Args: `prize`, `[channel]`, `[image_url]`*", inline=False) # Add drop command
         embed.add_field(name="/g profile", value="Shows giveaway statistics for a user.\n*Args: `[user]`*", inline=False) # Add profile command
         embed.add_field(name="/g list", value="Lists active giveaways in this server by sequential ID.", inline=False)
         embed.add_field(name="/g end", value="Ends a giveaway immediately.\n*Args: `giveaway_id`*", inline=False)
         embed.add_field(name="/g cancel", value="Cancels an active giveaway.\n*Args: `giveaway_id`*", inline=False)
         embed.add_field(name="/g reroll", value="Rerolls winners for a recently ended giveaway.\n*Args: `giveaway_id`*", inline=False)
         embed.add_field(name="/g settings", value="Configure server-specific settings.", inline=False) # Simplify args list due to length
         embed.add_field(name="ㅤ", value="*Use `/g settings` without args to view current settings. See command usage for available arguments.*", inline=False) # Add note about settings args

         embed.add_field(name="Button Usage (on giveaway message)", value=
                         "🎉 **Join:** Enter the giveaway. Click again to leave.\n" # Update join button description
                         "👥 **<Count>:** View the list of current participants.\n" # Update participants description
                         "End **(Red):** End the giveaway early (Host, Staff, or Manager only).\n" # Update end button description
                         "Reroll **(Blue/Purple, after end):** Reroll winners (Host, Staff, or Manager only).\n" # Add reroll button description
                         "🏆 **View Ended Giveaway (Link, after end):** Jump to the giveaway message." # Add link button description
                         , inline=False)

         embed.add_field(name="Formatting Help", value=
                          "**Duration/Cooldown:** `10s`, `15m`, `2h`, `1d`, `1h30m` (Cooldown max unit is hours).\n"
                          "**Bonus/Bypass Roles:** Mention the role(s). Example: `bonus_roles:@VIP:2 @Booster:1` `bypass_roles:@Admin @Mod`\n"
                          "**Keywords:** Comma-separated list. Example: `keywords:enter, giveaway, win`\n"
                          "**Settings Unset:** For roles/channels, select the 'Unset' option (value 0) from autocomplete. For default bypass, type `none`. For other optional text/image fields, omit the argument.\n" # Clarified unset method
                          "**Settings Formatting:** For embed/message text, use `{prize}`, `{winners}`, `{host}`, `{guild_name}`, `{giveaway_id}` where applicable. Markdown is supported in DM descriptions."
                          , inline=False)

         embed.set_footer(text="Remember to grant necessary permissions!")
         await interaction.response.send_message(embed=embed, ephemeral=True)


# -------------------------------------------------------------------
# Setup Function for Cog
# -------------------------------------------------------------------
async def setup(bot: commands.Bot):
    # Ensure bot has giveaway_cog attribute assigned before adding the cog
    # This might be needed if the cog is loaded via load_extension
    if not hasattr(bot, 'giveaway_cog'):
        bot.giveaway_cog = GiveawayCog(bot)
    await bot.add_cog(bot.giveaway_cog)
    logger.info("Giveaway Cog loaded successfully.")

# -------------------------------------------------------------------
# Example Bot Implementation (if running this file directly)
# -------------------------------------------------------------------
if __name__ == "__main__":
    # This part is for running the cog standalone for testing
    # In a real multi-cog bot, you'd load this cog from your main bot file

    # Configure logging to console immediately
    logging.basicConfig(level=logging.INFO, handlers=[stream_handler], format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')

    bot = commands.Bot(command_prefix=BOT_PREFIX or "!", intents=INTENTS)

    @bot.event
    async def on_ready():
        logger.info(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
        logger.info('------')
        # Load the cog
        # Assign giveaway_cog attribute BEFORE setup() is called
        if not hasattr(bot, 'giveaway_cog'):
             bot.giveaway_cog = GiveawayCog(bot)
        await bot.add_cog(bot.giveaway_cog)

        # Sync commands (important for slash commands to appear)
        # Might take a few minutes for Discord to update globally
        try:
            synced = await bot.tree.sync()
            logger.info(f"Synced {len(synced)} application commands globally.")
            # You might want to sync to specific guilds for faster testing during development:
            # test_guild_id = YOUR_TEST_GUILD_ID # Replace with your guild ID
            # test_guild = discord.Object(id=test_guild_id)
            # bot.tree.copy_global_to(guild=test_guild)
            # synced = await bot.tree.sync(guild=test_guild)
            # logger.info(f"Synced {len(synced)} commands to test guild {test_guild_id}.")

        except Exception as e:
            logger.error(f"Failed to sync application commands: {e}", exc_info=True)


    # Basic command to check if bot is responsive
    @bot.command()
    async def ping(ctx):
        await ctx.send(f'Pong! Latency: {round(bot.latency * 1000)}ms')

    # Error handler for slash commands (optional but good practice)
    @bot.tree.error
    async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            await interaction.response.send_message("You do not have the required permissions to use this command.", ephemeral=True)
        elif isinstance(error, app_commands.CommandInvokeError):
             logger.error(f"Error executing command {interaction.command.name} (Interaction ID: {interaction.id}): {error.original}", exc_info=True)
             # Attempt to respond or follow up if not already done
             if interaction.response.is_done():
                 await interaction.followup.send(f"An error occurred while running this command: {error.original}", ephemeral=True)
             else:
                 await interaction.response.send_message(f"An error occurred while running this command: {error.original}", ephemeral=True)
        else:
            logger.error(f"An unexpected error occurred: {error} (Interaction ID: {interaction.id})", exc_info=True)
            if interaction.response.is_done():
                 await interaction.followup.send("An unexpected error occurred.", ephemeral=True)
            else:
                await interaction.response.send_message("An unexpected error occurred.", ephemeral=True)


    # Run the bot
    try:
        bot.run(BOT_TOKEN)
    except discord.LoginFailure:
        logger.critical("Login failed: Improper token provided.")
    except Exception as e:
        logger.critical(f"Error running bot: {e}", exc_info=True)
