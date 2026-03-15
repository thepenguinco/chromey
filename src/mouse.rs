//! Smart mouse movement with human-like bezier curves and position tracking.
//!
//! This module provides realistic mouse movement simulation using cubic bezier
//! curves, configurable jitter, overshoot, and easing. The [`SmartMouse`] struct
//! tracks the current mouse position across operations so that every movement
//! starts from where the cursor actually is.

use crate::layout::Point;
use rand::Rng;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Configuration for smart mouse movement behavior.
#[derive(Debug, Clone)]
pub struct SmartMouseConfig {
    /// Number of intermediate steps for movement (higher = smoother). Default: 25.
    pub steps: usize,
    /// Overshoot factor past the target (0.0 = none, 1.0 = full distance). Default: 0.15.
    pub overshoot: f64,
    /// Per-step jitter in CSS pixels. Default: 1.5.
    pub jitter: f64,
    /// Base delay between movement steps in milliseconds. Default: 8.
    pub step_delay_ms: u64,
    /// Whether to apply ease-in-out timing (acceleration/deceleration). Default: true.
    pub easing: bool,
}

impl Default for SmartMouseConfig {
    fn default() -> Self {
        Self {
            steps: 25,
            overshoot: 0.15,
            jitter: 1.5,
            step_delay_ms: 8,
            easing: true,
        }
    }
}

/// A single step in a mouse movement path.
#[derive(Debug, Clone)]
pub struct MovementStep {
    /// The point to move to.
    pub point: Point,
    /// Delay before dispatching the next step.
    pub delay: Duration,
}

/// Evaluate a cubic bezier curve at parameter `t` in [0, 1].
///
/// Given control points P0, P1, P2, P3:
/// B(t) = (1-t)³·P0 + 3(1-t)²·t·P1 + 3(1-t)·t²·P2 + t³·P3
fn cubic_bezier(p0: Point, p1: Point, p2: Point, p3: Point, t: f64) -> Point {
    let inv = 1.0 - t;
    let inv2 = inv * inv;
    let inv3 = inv2 * inv;
    let t2 = t * t;
    let t3 = t2 * t;

    Point {
        x: inv3 * p0.x + 3.0 * inv2 * t * p1.x + 3.0 * inv * t2 * p2.x + t3 * p3.x,
        y: inv3 * p0.y + 3.0 * inv2 * t * p1.y + 3.0 * inv * t2 * p2.y + t3 * p3.y,
    }
}

/// Ease-in-out cubic function for natural acceleration/deceleration.
fn ease_in_out(t: f64) -> f64 {
    if t < 0.5 {
        4.0 * t * t * t
    } else {
        1.0 - (-2.0 * t + 2.0).powi(3) / 2.0
    }
}

/// Generate a human-like mouse movement path from `from` to `to`.
///
/// Returns a series of [`MovementStep`]s that, when dispatched sequentially,
/// produce a realistic-looking cursor trajectory with natural timing.
pub fn generate_path(from: Point, to: Point, config: &SmartMouseConfig) -> Vec<MovementStep> {
    let mut rng = rand::rng();
    let steps = config.steps.max(2);

    let dx = to.x - from.x;
    let dy = to.y - from.y;
    let distance = (dx * dx + dy * dy).sqrt();

    // For very short moves, just go directly
    if distance < 2.0 {
        return vec![MovementStep {
            point: to,
            delay: Duration::from_millis(config.step_delay_ms),
        }];
    }

    // Perpendicular unit vector for control point offsets
    let (perp_x, perp_y) = if distance > 0.001 {
        (-dy / distance, dx / distance)
    } else {
        (0.0, 1.0)
    };

    // Random control point offsets (curved path)
    let spread = distance * 0.3;
    let offset1: f64 = rng.random_range(-spread..spread);
    let offset2: f64 = rng.random_range(-spread..spread);

    let cp1 = Point {
        x: from.x + dx * 0.25 + perp_x * offset1,
        y: from.y + dy * 0.25 + perp_y * offset1,
    };
    let cp2 = Point {
        x: from.x + dx * 0.75 + perp_x * offset2,
        y: from.y + dy * 0.75 + perp_y * offset2,
    };

    // Determine whether to overshoot
    let should_overshoot = config.overshoot > 0.0 && distance > 10.0;

    let overshoot_target = if should_overshoot {
        let overshoot_amount = distance * config.overshoot * rng.random_range(0.5..1.5);
        Point {
            x: to.x + (dx / distance) * overshoot_amount,
            y: to.y + (dy / distance) * overshoot_amount,
        }
    } else {
        to
    };

    let main_steps = if should_overshoot {
        (steps as f64 * 0.85) as usize
    } else {
        steps
    };

    let mut path = Vec::with_capacity(steps + 2);

    // Main bezier path
    let end = if should_overshoot {
        overshoot_target
    } else {
        to
    };

    for i in 1..=main_steps {
        let raw_t = i as f64 / main_steps as f64;
        let t = if config.easing {
            ease_in_out(raw_t)
        } else {
            raw_t
        };

        let mut p = cubic_bezier(from, cp1, cp2, end, t);

        // Add jitter except near the end
        if config.jitter > 0.0 && i < main_steps.saturating_sub(2) {
            p.x += rng.random_range(-config.jitter..config.jitter);
            p.y += rng.random_range(-config.jitter..config.jitter);
        }

        // Vary delay for natural timing
        let delay_variation: f64 = rng.random_range(0.7..1.3);
        let delay = Duration::from_millis((config.step_delay_ms as f64 * delay_variation) as u64);

        path.push(MovementStep { point: p, delay });
    }

    // Correction steps back to actual target after overshoot
    if should_overshoot {
        let correction_steps = steps.saturating_sub(main_steps).max(3);
        let last = path.last().map(|s| s.point).unwrap_or(overshoot_target);

        for i in 1..=correction_steps {
            let t = i as f64 / correction_steps as f64;
            let t = if config.easing { ease_in_out(t) } else { t };

            let p = Point {
                x: last.x + (to.x - last.x) * t,
                y: last.y + (to.y - last.y) * t,
            };

            let delay = Duration::from_millis((config.step_delay_ms as f64 * 0.6) as u64);
            path.push(MovementStep { point: p, delay });
        }
    }

    // Ensure the final point is exactly the target
    if let Some(last) = path.last_mut() {
        last.point = to;
    }

    path
}

/// Tracks the current mouse position and generates human-like movement paths.
///
/// Uses lock-free atomics for interior mutability — safe to use behind `Arc`
/// and `&self` references without any mutex. Use this alongside CDP
/// `Input.dispatchMouseEvent` calls so that every movement starts from
/// the real cursor location instead of teleporting.
pub struct SmartMouse {
    pos_x: AtomicU64,
    pos_y: AtomicU64,
    config: SmartMouseConfig,
}

impl std::fmt::Debug for SmartMouse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pos = self.position();
        f.debug_struct("SmartMouse")
            .field("position", &pos)
            .field("config", &self.config)
            .finish()
    }
}

impl SmartMouse {
    /// Create a new `SmartMouse` starting at (0, 0) with default configuration.
    pub fn new() -> Self {
        Self {
            pos_x: AtomicU64::new(0.0_f64.to_bits()),
            pos_y: AtomicU64::new(0.0_f64.to_bits()),
            config: SmartMouseConfig::default(),
        }
    }

    /// Create a `SmartMouse` with custom configuration.
    pub fn with_config(config: SmartMouseConfig) -> Self {
        Self {
            pos_x: AtomicU64::new(0.0_f64.to_bits()),
            pos_y: AtomicU64::new(0.0_f64.to_bits()),
            config,
        }
    }

    /// Get the current tracked mouse position.
    pub fn position(&self) -> Point {
        Point {
            x: f64::from_bits(self.pos_x.load(Ordering::Relaxed)),
            y: f64::from_bits(self.pos_y.load(Ordering::Relaxed)),
        }
    }

    /// Set the mouse position directly (e.g., after a teleport or click).
    pub fn set_position(&self, point: Point) {
        self.pos_x.store(point.x.to_bits(), Ordering::Relaxed);
        self.pos_y.store(point.y.to_bits(), Ordering::Relaxed);
    }

    /// Get the movement configuration.
    pub fn config(&self) -> &SmartMouseConfig {
        &self.config
    }

    /// Generate a movement path from the current position to `target`.
    ///
    /// This updates the tracked position to `target` and returns a series of
    /// [`MovementStep`]s for dispatching intermediate `MouseMoved` events.
    pub fn path_to(&self, target: Point) -> Vec<MovementStep> {
        let from = self.position();
        self.set_position(target);
        generate_path(from, target, &self.config)
    }
}

impl Default for SmartMouse {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cubic_bezier_endpoints() {
        let p0 = Point::new(0.0, 0.0);
        let p1 = Point::new(25.0, 50.0);
        let p2 = Point::new(75.0, 50.0);
        let p3 = Point::new(100.0, 100.0);

        let start = cubic_bezier(p0, p1, p2, p3, 0.0);
        assert!((start.x - p0.x).abs() < 1e-10);
        assert!((start.y - p0.y).abs() < 1e-10);

        let end = cubic_bezier(p0, p1, p2, p3, 1.0);
        assert!((end.x - p3.x).abs() < 1e-10);
        assert!((end.y - p3.y).abs() < 1e-10);
    }

    #[test]
    fn test_cubic_bezier_midpoint() {
        // Straight line: control points on the line
        let p0 = Point::new(0.0, 0.0);
        let p1 = Point::new(33.3, 33.3);
        let p2 = Point::new(66.6, 66.6);
        let p3 = Point::new(100.0, 100.0);

        let mid = cubic_bezier(p0, p1, p2, p3, 0.5);
        // Should be approximately (50, 50) for a straight-line bezier
        assert!((mid.x - 50.0).abs() < 1.0);
        assert!((mid.y - 50.0).abs() < 1.0);
    }

    #[test]
    fn test_ease_in_out_boundaries() {
        assert!((ease_in_out(0.0)).abs() < 1e-10);
        assert!((ease_in_out(1.0) - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_ease_in_out_midpoint() {
        let mid = ease_in_out(0.5);
        assert!((mid - 0.5).abs() < 1e-10);
    }

    #[test]
    fn test_ease_in_out_monotonic() {
        let mut prev = 0.0;
        for i in 1..=100 {
            let t = i as f64 / 100.0;
            let val = ease_in_out(t);
            assert!(
                val >= prev,
                "ease_in_out should be monotonically increasing"
            );
            prev = val;
        }
    }

    #[test]
    fn test_generate_path_ends_at_target() {
        let from = Point::new(10.0, 20.0);
        let to = Point::new(500.0, 300.0);
        let config = SmartMouseConfig::default();

        let path = generate_path(from, to, &config);

        assert!(!path.is_empty());
        let last = &path.last().unwrap().point;
        assert!(
            (last.x - to.x).abs() < 1e-10 && (last.y - to.y).abs() < 1e-10,
            "path must end exactly at target, got ({}, {})",
            last.x,
            last.y
        );
    }

    #[test]
    fn test_generate_path_short_distance() {
        let from = Point::new(100.0, 100.0);
        let to = Point::new(100.5, 100.5);
        let config = SmartMouseConfig::default();

        let path = generate_path(from, to, &config);

        assert_eq!(
            path.len(),
            1,
            "very short moves should produce a single step"
        );
        assert!((path[0].point.x - to.x).abs() < 1e-10);
        assert!((path[0].point.y - to.y).abs() < 1e-10);
    }

    #[test]
    fn test_generate_path_no_overshoot() {
        let from = Point::new(0.0, 0.0);
        let to = Point::new(200.0, 200.0);
        let config = SmartMouseConfig {
            overshoot: 0.0,
            ..Default::default()
        };

        let path = generate_path(from, to, &config);
        assert_eq!(path.len(), config.steps);
    }

    #[test]
    fn test_generate_path_no_jitter() {
        let from = Point::new(0.0, 0.0);
        let to = Point::new(200.0, 200.0);
        let config = SmartMouseConfig {
            jitter: 0.0,
            overshoot: 0.0,
            easing: false,
            ..Default::default()
        };

        // Without jitter or easing, successive runs with same from/to
        // should produce paths that lie on a bezier curve (no random noise
        // except from control point placement).
        let path = generate_path(from, to, &config);
        assert!(!path.is_empty());
        let last = &path.last().unwrap().point;
        assert!((last.x - to.x).abs() < 1e-10);
        assert!((last.y - to.y).abs() < 1e-10);
    }

    #[test]
    fn test_generate_path_step_count_with_overshoot() {
        let from = Point::new(0.0, 0.0);
        let to = Point::new(500.0, 500.0);
        let config = SmartMouseConfig {
            steps: 30,
            overshoot: 0.2,
            ..Default::default()
        };

        let path = generate_path(from, to, &config);
        // With overshoot: main_steps (~85%) + correction_steps (~15%, min 3)
        assert!(path.len() >= config.steps);
    }

    #[test]
    fn test_generate_path_no_huge_jumps() {
        let from = Point::new(0.0, 0.0);
        let to = Point::new(300.0, 300.0);
        let config = SmartMouseConfig {
            steps: 50,
            overshoot: 0.0,
            jitter: 0.0,
            ..Default::default()
        };

        let path = generate_path(from, to, &config);

        let mut prev = from;
        let max_distance = (300.0_f64 * 300.0 + 300.0 * 300.0).sqrt(); // total distance

        for step in &path {
            let dx = step.point.x - prev.x;
            let dy = step.point.y - prev.y;
            let step_dist = (dx * dx + dy * dy).sqrt();
            // No single step should jump more than half the total distance
            assert!(
                step_dist < max_distance * 0.6,
                "step jumped {} pixels (max total: {})",
                step_dist,
                max_distance
            );
            prev = step.point;
        }
    }

    #[test]
    fn test_smart_mouse_position_tracking() {
        let mouse = SmartMouse::new();

        assert_eq!(mouse.position(), Point::new(0.0, 0.0));

        mouse.set_position(Point::new(100.0, 200.0));
        assert_eq!(mouse.position(), Point::new(100.0, 200.0));
    }

    #[test]
    fn test_smart_mouse_path_to_updates_position() {
        let mouse = SmartMouse::new();
        let target = Point::new(500.0, 300.0);

        let path = mouse.path_to(target);
        assert!(!path.is_empty());

        // Position should now be at the target
        assert_eq!(mouse.position(), target);
    }

    #[test]
    fn test_smart_mouse_consecutive_paths() {
        let mouse = SmartMouse::with_config(SmartMouseConfig {
            overshoot: 0.0,
            jitter: 0.0,
            ..Default::default()
        });

        let target1 = Point::new(100.0, 100.0);
        let path1 = mouse.path_to(target1);
        assert!(!path1.is_empty());
        assert_eq!(mouse.position(), target1);

        let target2 = Point::new(400.0, 300.0);
        let _path2 = mouse.path_to(target2);
        assert_eq!(mouse.position(), target2);
    }

    #[test]
    fn test_smart_mouse_same_position_no_move() {
        let mouse = SmartMouse::new();
        mouse.set_position(Point::new(100.0, 100.0));

        let path = mouse.path_to(Point::new(100.0, 100.0));
        // Zero distance should produce a single direct step
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn test_smart_mouse_custom_config() {
        let config = SmartMouseConfig {
            steps: 10,
            overshoot: 0.0,
            jitter: 0.0,
            step_delay_ms: 16,
            easing: false,
        };

        let mouse = SmartMouse::with_config(config.clone());
        let path = mouse.path_to(Point::new(200.0, 200.0));

        assert_eq!(path.len(), config.steps);
    }

    #[test]
    fn test_movement_delays_are_reasonable() {
        let config = SmartMouseConfig {
            step_delay_ms: 10,
            ..Default::default()
        };

        let path = generate_path(Point::new(0.0, 0.0), Point::new(500.0, 500.0), &config);

        for step in &path {
            // Delays should be within 0-30ms range for a 10ms base
            assert!(
                step.delay.as_millis() <= 30,
                "delay too large: {:?}",
                step.delay
            );
        }
    }

    #[test]
    fn test_default_config() {
        let config = SmartMouseConfig::default();
        assert_eq!(config.steps, 25);
        assert!((config.overshoot - 0.15).abs() < 1e-10);
        assert!((config.jitter - 1.5).abs() < 1e-10);
        assert_eq!(config.step_delay_ms, 8);
        assert!(config.easing);
    }
}
