//! Voltra V1 game-runtime composition model.
//!
//! This module is the first foundation for the low-latency runtime direction:
//! lobbies, ECS, AOI, delta fanout, and gameplay modules are explicit concepts
//! instead of being hidden inside one large "game-ready" template.

use std::collections::{BTreeSet, HashMap};

/// Where a module's state belongs on the latency spectrum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeDomain {
    /// Hot per-tick simulation state. This belongs in lobby-owned ECS storage.
    HotSimulation,
    /// Durable gameplay state that can use reducer/TableStore semantics.
    DurableGameplay,
    /// Control-plane state outside the tick path.
    ControlPlane,
    /// Client integration files and SDK glue.
    Client,
}

/// A reusable gameplay/runtime module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeModule {
    pub id: &'static str,
    pub domain: RuntimeDomain,
    pub description: &'static str,
    pub dependencies: &'static [&'static str],
}

/// A genre preset is only a recipe: studios can add/remove modules freely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenreRecipe {
    pub id: &'static str,
    pub description: &'static str,
    pub modules: &'static [&'static str],
}

/// A resolved module set for a project scaffold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeComposition {
    pub modules: Vec<&'static RuntimeModule>,
}

/// Built-in Voltra V1 module catalog.
pub fn builtin_modules() -> &'static [RuntimeModule] {
    &MODULES
}

/// Built-in genre recipes. These are starting points, not cages.
pub fn builtin_genres() -> &'static [GenreRecipe] {
    &GENRES
}

/// Resolve a genre plus extra modules into a dependency-closed composition.
pub fn compose_runtime(
    genre: Option<&str>,
    extra_modules: &[&str],
) -> Result<RuntimeComposition, String> {
    let by_id: HashMap<&str, &RuntimeModule> =
        MODULES.iter().map(|module| (module.id, module)).collect();

    let mut requested = BTreeSet::new();
    if let Some(genre_id) = genre {
        let recipe = GENRES
            .iter()
            .find(|recipe| recipe.id == genre_id)
            .ok_or_else(|| format!("unknown genre recipe '{genre_id}'"))?;
        requested.extend(recipe.modules.iter().copied());
    }
    requested.extend(extra_modules.iter().copied());

    let mut resolved = BTreeSet::new();
    for module_id in requested {
        resolve_module(module_id, &by_id, &mut resolved)?;
    }

    let modules = resolved
        .into_iter()
        .map(|module_id| by_id[module_id])
        .collect();
    Ok(RuntimeComposition { modules })
}

fn resolve_module<'a>(
    module_id: &'a str,
    by_id: &HashMap<&'a str, &'a RuntimeModule>,
    resolved: &mut BTreeSet<&'a str>,
) -> Result<(), String> {
    if resolved.contains(module_id) {
        return Ok(());
    }
    let module = by_id
        .get(module_id)
        .ok_or_else(|| format!("unknown runtime module '{module_id}'"))?;
    for dep in module.dependencies {
        resolve_module(dep, by_id, resolved)?;
    }
    resolved.insert(module.id);
    Ok(())
}

const MODULES: [RuntimeModule; 21] = [
    RuntimeModule {
        id: "sessions",
        domain: RuntimeDomain::ControlPlane,
        description: "Session-token validation and local identity cache",
        dependencies: &[],
    },
    RuntimeModule {
        id: "lobby",
        domain: RuntimeDomain::HotSimulation,
        description: "Authoritative lobby ownership and routing",
        dependencies: &["sessions"],
    },
    RuntimeModule {
        id: "tick",
        domain: RuntimeDomain::HotSimulation,
        description: "Independent per-lobby tick scheduling",
        dependencies: &["lobby"],
    },
    RuntimeModule {
        id: "ecs",
        domain: RuntimeDomain::HotSimulation,
        description: "Dense component storage for hot simulation state",
        dependencies: &["tick"],
    },
    RuntimeModule {
        id: "aoi",
        domain: RuntimeDomain::HotSimulation,
        description: "Area-of-interest filtering and spatial subscriptions",
        dependencies: &["ecs"],
    },
    RuntimeModule {
        id: "delta",
        domain: RuntimeDomain::HotSimulation,
        description: "Compact delta encoding and fanout",
        dependencies: &["aoi"],
    },
    RuntimeModule {
        id: "runtime-persistence",
        domain: RuntimeDomain::DurableGameplay,
        description: "Append-only runtime log and lobby snapshots",
        dependencies: &["lobby"],
    },
    RuntimeModule {
        id: "movement",
        domain: RuntimeDomain::HotSimulation,
        description: "Player transform, velocity, and input application",
        dependencies: &["ecs", "delta"],
    },
    RuntimeModule {
        id: "weapons",
        domain: RuntimeDomain::HotSimulation,
        description: "Weapon state, fire commands, cooldowns, and ammo",
        dependencies: &["movement"],
    },
    RuntimeModule {
        id: "combat",
        domain: RuntimeDomain::HotSimulation,
        description: "Damage, health, death, respawn, and combat events",
        dependencies: &["weapons"],
    },
    RuntimeModule {
        id: "hit-detection",
        domain: RuntimeDomain::HotSimulation,
        description: "Latency-aware hit validation and collision queries",
        dependencies: &["combat"],
    },
    RuntimeModule {
        id: "inventory",
        domain: RuntimeDomain::DurableGameplay,
        description: "Durable player item stacks and item grants",
        dependencies: &["runtime-persistence"],
    },
    RuntimeModule {
        id: "equipment",
        domain: RuntimeDomain::DurableGameplay,
        description: "Equipped items with hot stat projection into ECS",
        dependencies: &["inventory", "ecs"],
    },
    RuntimeModule {
        id: "economy",
        domain: RuntimeDomain::DurableGameplay,
        description: "Currency, shops, trades, and reward transactions",
        dependencies: &["inventory"],
    },
    RuntimeModule {
        id: "quests",
        domain: RuntimeDomain::DurableGameplay,
        description: "Quest state, progress tracking, and rewards",
        dependencies: &["runtime-persistence"],
    },
    RuntimeModule {
        id: "guilds",
        domain: RuntimeDomain::DurableGameplay,
        description: "Guild membership, roles, invites, and moderation",
        dependencies: &["runtime-persistence"],
    },
    RuntimeModule {
        id: "parties",
        domain: RuntimeDomain::ControlPlane,
        description: "Party queueing, invites, and shared matchmaking entry",
        dependencies: &["sessions"],
    },
    RuntimeModule {
        id: "matchmaking",
        domain: RuntimeDomain::ControlPlane,
        description: "Queues, reservations, backfill, and lobby allocation",
        dependencies: &["parties", "lobby"],
    },
    RuntimeModule {
        id: "chat",
        domain: RuntimeDomain::DurableGameplay,
        description: "Rooms, messages, reactions, moderation, and presence hooks",
        dependencies: &["sessions"],
    },
    RuntimeModule {
        id: "leaderboard",
        domain: RuntimeDomain::DurableGameplay,
        description: "Score submission, ranking, seasons, and resets",
        dependencies: &["runtime-persistence"],
    },
    RuntimeModule {
        id: "replay",
        domain: RuntimeDomain::DurableGameplay,
        description: "Tick input/delta capture for debugging and spectating",
        dependencies: &["delta", "runtime-persistence"],
    },
];

const GENRES: [GenreRecipe; 7] = [
    GenreRecipe {
        id: "fps",
        description: "Low-latency match lobbies with movement, weapons, combat, and hit validation",
        modules: &["movement", "weapons", "combat", "hit-detection", "matchmaking", "leaderboard", "replay"],
    },
    GenreRecipe {
        id: "mmo",
        description: "Persistent world gameplay with inventory, equipment, economy, quests, guilds, and chat",
        modules: &["movement", "combat", "inventory", "equipment", "economy", "quests", "guilds", "chat"],
    },
    GenreRecipe {
        id: "battle-royale",
        description: "Large lobby survival loop with combat, matchmaking, replay, and ranking",
        modules: &["movement", "weapons", "combat", "hit-detection", "matchmaking", "leaderboard", "replay"],
    },
    GenreRecipe {
        id: "survival",
        description: "Persistent survival gameplay with combat, inventory, crafting-ready economy, and parties",
        modules: &["movement", "combat", "inventory", "equipment", "economy", "parties", "chat"],
    },
    GenreRecipe {
        id: "racing",
        description: "Low-latency movement lobbies with ranking and replay capture",
        modules: &["movement", "matchmaking", "leaderboard", "replay"],
    },
    GenreRecipe {
        id: "moba",
        description: "Team lobby combat with matchmaking, equipment projection, and ranking",
        modules: &["movement", "combat", "equipment", "matchmaking", "leaderboard", "replay"],
    },
    GenreRecipe {
        id: "social",
        description: "Presence-first rooms with chat, parties, and durable social state",
        modules: &["lobby", "parties", "chat"],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fps_recipe_resolves_core_hot_path_dependencies() {
        let composition = compose_runtime(Some("fps"), &[]).unwrap();
        let ids: BTreeSet<_> = composition.modules.iter().map(|module| module.id).collect();
        for expected in [
            "sessions", "lobby", "tick", "ecs", "aoi", "delta", "movement",
        ] {
            assert!(ids.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn extra_modules_are_dependency_closed() {
        let composition = compose_runtime(None, &["equipment"]).unwrap();
        let ids: BTreeSet<_> = composition.modules.iter().map(|module| module.id).collect();
        for expected in [
            "sessions",
            "lobby",
            "tick",
            "ecs",
            "runtime-persistence",
            "inventory",
            "equipment",
        ] {
            assert!(ids.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn unknown_module_is_reported() {
        let err = compose_runtime(None, &["does-not-exist"]).unwrap_err();
        assert!(err.contains("unknown runtime module"));
    }
}
