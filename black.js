(() => {
  const now = document.querySelector(".panel--now");
  const flow = document.querySelector(".panel--flow");
  const tabs = document.querySelector(".flow-tabs");
  const controls = document.querySelector(".controls");
  const progress = document.querySelector(".progress");
  const progressTime = document.querySelector(".progress-time");
  const shell = document.querySelector(".shell");
  const stage = document.querySelector("#nowContent");
  if (!now || !flow || !tabs || !controls || !progress || !progressTime || !shell || !stage) return;

  const dock = document.createElement("nav");
  dock.className = "black-dock";
  dock.setAttribute("aria-label", "Shared flow");
  dock.append(tabs);
  shell.prepend(dock);

  const panel = document.createElement("div");
  panel.className = "black-panel";
  panel.id = "blackPanel";
  panel.hidden = true;
  flow.before(panel);
  panel.append(flow);
  let closingAnimation;
  function closePanel() {
    if (panel.hidden || closingAnimation) return;
    const activeTab = tabs.querySelector('[aria-expanded="true"]');
    if (panel.contains(document.activeElement)) activeTab?.focus();
    activeTab?.setAttribute("aria-expanded", "false");
    panel.inert = true;
    document.body.classList.remove("black-panel-open");
    const animation = panel.animate(
      [{ opacity: 1, transform: "translateY(0)" }, { opacity: 0, transform: "translateY(-8px)" }],
      { duration: 220, easing: "cubic-bezier(.4, 0, .6, 1)" },
    );
    closingAnimation = animation;
    animation.onfinish = () => {
      if (closingAnimation !== animation) return;
      panel.hidden = true;
      panel.dataset.tab = "";
      closingAnimation = null;
    };
  }

  function togglePanel(button) {
    if (!panel.hidden && !closingAnimation && panel.dataset.tab === button.dataset.flowTab) {
      closePanel();
      return;
    }
    if (closingAnimation) {
      closingAnimation.cancel();
      closingAnimation = null;
    }
    panel.hidden = false;
    panel.inert = false;
    panel.dataset.tab = button.dataset.flowTab;
    document.body.classList.add("black-panel-open");
    tabs.querySelectorAll(".flow-tab").forEach((tab) => {
      tab.setAttribute("aria-expanded", String(tab === button));
    });
  }

  tabs.addEventListener("click", (event) => {
    const button = event.target.closest(".flow-tab");
    if (button) togglePanel(button);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closePanel();
  });

  const icons = {
    add: '<circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/>',
    queue: '<path d="M3 5h13M3 12h9M3 19h9M21 16V5"/><circle cx="18" cy="16" r="3"/>',
    recent: '<path d="M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5M12 7v5l4 2"/>',
    discover: '<path d="M4 4v16M8 8v12M12 6v14M16 6l4 14"/>',
    device: '<rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8M12 17v4"/>',
  };
  tabs.querySelectorAll(".flow-tab").forEach((button) => {
    const name = button.dataset.flowTab;
    const label = button.childNodes[0]?.textContent.trim() || name;
    button.setAttribute("aria-label", label);
    button.setAttribute("aria-controls", "blackPanel");
    button.setAttribute("aria-expanded", "false");
    button.title = label;
    if (name === "favorites") {
      button.insertAdjacentHTML("afterbegin", ICONS.favorite);
      return;
    }
    const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    icon.setAttribute("viewBox", "0 0 24 24");
    icon.setAttribute("aria-hidden", "true");
    icon.innerHTML = icons[name] || "";
    button.prepend(icon);
  });
  const deviceTab = document.querySelector("#deviceTab");
  if (deviceTab) {
    const keepDeviceIcon = () => {
      if (deviceTab.querySelector("svg")) return;
      const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      icon.setAttribute("viewBox", "0 0 24 24");
      icon.setAttribute("aria-hidden", "true");
      icon.innerHTML = icons.device;
      deviceTab.prepend(icon);
    };
    new MutationObserver(keepDeviceIcon).observe(deviceTab, { childList: true });
    if (!localStorage.getItem("selfStationDeviceName") && !localStorage.getItem("jukeboxDeviceName")) {
      togglePanel(deviceTab);
    }
  }

  const deck = document.createElement("div");
  deck.className = "black-deck";
  const timeline = document.createElement("div");
  timeline.className = "black-timeline";
  timeline.append(progressTime, progress);
  deck.append(timeline, controls);
  shell.append(deck);
  new ResizeObserver(() => {
    document.body.style.setProperty("--deck-height", `${deck.offsetHeight}px`);
  }).observe(deck);
  const liveLabel = document.createElement("span");
  liveLabel.className = "black-live-label";
  liveLabel.textContent = "LIVE";
  controls.querySelector(".volume").prepend(liveLabel);
  const currentTime = document.querySelector("#currentTime");
  new MutationObserver(() => {
    deck.classList.toggle("black-deck-live", currentTime.textContent === "LIVE");
  }).observe(currentTime, { childList: true });

  const syncLine = document.querySelector(".sync-line");
  if (syncLine) deck.append(syncLine);

  fetch("/in-the-black/lucide-icons.json").then((response) => {
    if (!response.ok) throw new Error("Icon library unavailable");
    return response.json();
  }).then((library) => {
    const buttons = {
      playPauseButton: () => document.querySelector("#playPauseButton").getAttribute("aria-label") === "Play" ? "Play" : "Pause",
      skipButton: () => "SkipForward",
      requeueCurrentButton: () => "Plus",
      lyricsToggle: () => "Music2",
      wikiToggle: () => "Info",
      downloadToggle: () => "Download",
      openTrackButton: () => "ExternalLink",
      autoRadioToggle: () => "Radio",
      audioGate: () => "Volume2",
    };
    Object.entries(buttons).forEach(([id, name]) => {
      const button = document.getElementById(id);
      if (!button) return;
      const update = () => {
        const iconName = name();
        if (button.dataset.blackIcon === iconName && button.querySelector("svg.lucide")) return;
        button.innerHTML = library[iconName];
        button.dataset.blackIcon = iconName;
      };
      new MutationObserver(update).observe(button, { childList: true, attributes: true, attributeFilter: ["aria-label"] });
      update();
    });
  }).catch(() => {});

  const volume = document.querySelector("#volume");
  const volumeWrap = document.querySelector("#volumeRangeWrap");
  if (volume && volumeWrap) {
    const bars = document.createElement("span");
    bars.className = "black-wake";
    bars.setAttribute("aria-hidden", "true");
    for (let i = 0; i < 32; i += 1) bars.append(document.createElement("i"));
    volumeWrap.prepend(bars);
    let previous = Number(volume.value);
    let lastTime = performance.now();
    let resetTimer;
    const updateVolume = (animate) => {
      const value = Number(volume.value);
      const nowTime = performance.now();
      const speed = Math.min(1, Math.abs(value - previous) / Math.max(1, nowTime - lastTime) * 28);
      [...bars.children].forEach((bar, i) => {
        const position = i / 31 * 100;
        const wake = animate ? Math.max(0, 1 - Math.abs(position - value) / 25) * (7 + speed * 18) : 0;
        bar.style.height = 8 + wake + "px";
        bar.classList.toggle("filled", position <= value);
      });
      previous = value;
      lastTime = nowTime;
      clearTimeout(resetTimer);
      resetTimer = setTimeout(() => [...bars.children].forEach((bar) => { bar.style.height = "8px"; }), 220);
    };
    volume.addEventListener("input", () => updateVolume(true));
    volume.addEventListener("change", () => updateVolume(false));
    updateVolume(false);
  }

  document.querySelectorAll("#searchInput, #radioSearchInput, #deviceInlineInput, #spotifyProfileInput").forEach((input) => {
    const wrapper = document.createElement("label");
    wrapper.className = "black-field";
    const label = document.createElement("span");
    label.textContent = input.getAttribute("aria-label") || input.placeholder;
    if (!input.placeholder) input.placeholder = " ";
    input.before(wrapper);
    wrapper.append(input, label);
  });

  let tiltedCard;
  const tiltCardSelector = ".black-card-list > :is(.queue-item, .rank-item, .result):not(.list-placeholder)";

  function clearTilt() {
    tiltedCard?.style.removeProperty("--tilt-x");
    tiltedCard?.style.removeProperty("--tilt-y");
    tiltedCard = null;
  }

  panel.addEventListener("pointermove", (event) => {
    if (event.pointerType === "touch") return;
    const card = event.target.closest(tiltCardSelector);
    if (card !== tiltedCard) clearTilt();
    if (!card) return;

    tiltedCard = card;
    const rect = card.getBoundingClientRect();
    const x = Math.max(-.5, Math.min(.5, (event.clientX - rect.left) / rect.width - .5));
    const y = Math.max(-.5, Math.min(.5, (event.clientY - rect.top) / rect.height - .5));
    card.style.setProperty("--tilt-x", `${-y * 10}deg`);
    card.style.setProperty("--tilt-y", `${x * 12}deg`);
  });
  panel.addEventListener("pointerleave", clearTilt);
  panel.addEventListener("scroll", clearTilt, { capture: true, passive: true });

  const cardLists = {
    queue: null,
    recentTracks: "recent",
    favoriteTracks: "favorites",
    discoverTracks: null,
    fixedDiscoverTracks: null,
    results: "search",
    recommendationResults: null,
    radioResults: null,
    podcastResults: null,
  };
  for (const [id, name] of Object.entries(cardLists)) {
    const list = document.getElementById(id);
    if (!list) continue;
    list.classList.add("black-card-list");
    const source = name ? pagedLists[name] : null;
    const more = document.createElement("button");
    more.type = "button";
    more.className = "btn black-card-more";
    more.setAttribute("aria-label", "Load more");
    more.title = "Load more";
    more.innerHTML = lucide('<path d="m7 6 5 5 5-5"/><path d="m7 13 5 5 5-5"/>');
    let query = source?.query;

    function updateList() {
      if (query !== source?.query) {
        query = source?.query;
        list.scrollTop = 0;
      }
      if (source?.initialized && source.items.length && source.hasMore) {
        if (list.lastElementChild !== more) list.append(more);
      } else {
        more.remove();
      }
      more.disabled = Boolean(source?.loading);
    }

    more.addEventListener("click", async () => {
      const offset = list.scrollTop;
      more.disabled = true;
      try {
        await loadPagedList(name);
      } finally {
        updateList();
        list.scrollTop = offset;
      }
    });
    new MutationObserver(updateList).observe(list, {
      childList: true,
      attributes: true,
      attributeFilter: ["hidden"],
    });
    updateList();
  }

  const tooltip = document.createElement("div");
  tooltip.className = "black-tooltip";
  tooltip.setAttribute("aria-hidden", "true");
  tooltip.hidden = true;
  document.body.append(tooltip);
  let tooltipButton;

  function hideTooltip() {
    tooltip.hidden = true;
    tooltipButton = null;
  }

  function showTooltip(button) {
    if (!button?.isConnected || button === tooltipButton) return;
    const title = button.getAttribute("title");
    const label = button.getAttribute("aria-label") || title || button.dataset.tooltip || button.textContent.trim();
    if (!label) return;
    if (title) {
      button.dataset.tooltip = title;
      button.removeAttribute("title");
    }
    tooltipButton = button;
    tooltip.textContent = label;
    tooltip.hidden = false;
    const rect = button.getBoundingClientRect();
    const width = tooltip.offsetWidth;
    const height = tooltip.offsetHeight;
    const left = Math.min(innerWidth - width - 8, Math.max(8, rect.left + (rect.width - width) / 2));
    const below = rect.bottom + 8;
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${below + height <= innerHeight - 8 ? below : Math.max(8, rect.top - height - 8)}px`;
  }

  function buttonAt(target) {
    return target instanceof Element ? target.closest("button, [role='button']") : null;
  }

  document.addEventListener("pointerover", (event) => {
    if (event.pointerType !== "touch") showTooltip(buttonAt(event.target));
  });
  document.addEventListener("pointerout", (event) => {
    if (buttonAt(event.target) === tooltipButton && buttonAt(event.relatedTarget) !== tooltipButton) hideTooltip();
  });
  document.addEventListener("focusin", (event) => {
    const button = buttonAt(event.target);
    if (button?.matches(":focus-visible")) showTooltip(button);
  });
  document.addEventListener("focusout", hideTooltip);
  document.addEventListener("pointerdown", hideTooltip);
  document.addEventListener("scroll", hideTooltip, { capture: true, passive: true });
  window.addEventListener("resize", hideTooltip);
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") hideTooltip();
  });

  function makeRadioPicker(select, label) {
    if (!select || select.hidden) return;
    const group = document.createElement("fieldset");
    group.className = "black-radio-picker";
    group.setAttribute("aria-label", label);
    const hint = select.getAttribute("aria-describedby");
    if (hint) group.setAttribute("aria-describedby", hint);

    for (const option of select.options) {
      const segment = document.createElement("label");
      const radio = document.createElement("input");
      radio.type = "radio";
      radio.name = select.id + "-choice";
      radio.value = option.value;
      radio.checked = option.selected;
      radio.disabled = option.disabled || select.disabled;
      const text = document.createElement("span");
      text.textContent = option.label;
      segment.append(radio, text);
      group.append(segment);
    }

    group.addEventListener("change", (event) => {
      select.value = event.target.value;
      select.dispatchEvent(new Event("change", { bubbles: true }));
    });
    select.hidden = true;
    select.after(group);
  }

  makeRadioPicker(document.getElementById("audioMode"), "Audio mode");
  makeRadioPicker(document.getElementById("audioIntensity"), "Intensity");
  const playlistPreview = document.getElementById("playlistPreview");
  if (playlistPreview) {
    const update = () => makeRadioPicker(playlistPreview.querySelector("#playlistLimit"), "Track count");
    new MutationObserver(update).observe(playlistPreview, { childList: true });
    update();
  }

  const backdrop = document.querySelector("#pageBackdrop");
  if (backdrop) {
    let previousArtwork = backdrop.style.backgroundImage;
    new MutationObserver(() => {
      const nextArtwork = backdrop.style.backgroundImage;
      if (nextArtwork === previousArtwork) return;
      previousArtwork = nextArtwork;
      backdrop.getAnimations().forEach((animation) => animation.cancel());
      backdrop.animate([{ opacity: .1, filter: "blur(28px) brightness(.38)" }, { opacity: .4, filter: "blur(7px) brightness(.6)" }], { duration: 850, easing: "ease-out" });
    }).observe(backdrop, { attributes: true, attributeFilter: ["style"] });
  }
})();
