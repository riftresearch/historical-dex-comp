const PROVIDER_ORDER = ["lifi", "relay", "thorchain", "chainflip", "garden", "nearintents", "kyberswap"];
const PROVIDER_COLORS = {
  lifi: "#FF6F00",
  relay: "#00E5FF",
  thorchain: "#76FF03",
  chainflip: "#FFD600",
  garden: "#FF1744",
  nearintents: "#7C4DFF",
  kyberswap: "#00C853",
};
const SHORTFALL_NOTIONAL_PIVOT_USD = 1_000;
const SHORTFALL_NOTIONAL_PIVOT_SHARE = 0.25;

const state = {
  charts: {
    shortfall: null,
  },
  activeShortfallView: "usdc_eth_to_btc_bitcoin_all",
  shortfallZoom: {
    startValue: null,
    endValue: null,
  },
  shortfallSnapshot: null,
  shortfallLegendSelected: {},
  shortfallYAxisScale: "signed_log",
  shortfallShowBand: false,
};

const statusText = document.getElementById("status-text");
const providersBody = document.getElementById("providers-body");
const refreshBtn = document.getElementById("refresh-btn");
const shortfallViewSelect = document.getElementById("shortfall-view-select");
const shortfallYScaleSelect = document.getElementById("shortfall-y-scale-select");
const shortfallBandToggle = document.getElementById("shortfall-band-toggle");
const shortfallQuantileChartEl = document.getElementById("shortfall-quantile-chart");

function formatInt(value) {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatUsd(value) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatRate(value) {
  return `${Number(value).toFixed(2)}%`;
}

function formatBps(value, digits = 2) {
  if (!Number.isFinite(value)) {
    return "-";
  }
  return `${Number(value).toFixed(digits)} bps`;
}

function formatTime(value) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "-";
  }
  return parsed.toLocaleString("en-US", {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZoneName: "short",
  });
}

function formatAnalyzedRange(startAt, endAt) {
  const startLabel = formatTime(startAt);
  const endLabel = formatTime(endAt);
  if (startLabel === "-" && endLabel === "-") {
    return "-";
  }
  return `${startLabel} -> ${endLabel}`;
}

function formatUsdTick(value) {
  if (!Number.isFinite(value) || value <= 0) {
    return "";
  }
  if (value >= 1_000_000_000) {
    return `${(value / 1_000_000_000).toFixed(value >= 10_000_000_000 ? 0 : 1)}b`;
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}m`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(value >= 10_000 ? 0 : 1)}k`;
  }
  return value.toFixed(0);
}

function formatUsdPrecise(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }
  const maximumFractionDigits = value >= 100 ? 0 : 2;
  return new Intl.NumberFormat("en-US", { maximumFractionDigits }).format(value);
}

function formatUsdRange(lowerUsd, upperUsd) {
  return `$${formatUsdPrecise(lowerUsd)}-$${formatUsdPrecise(upperUsd)}`;
}

function toFinitePositiveOrNull(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const normalized = Number(value);
  if (normalized <= 0) {
    return null;
  }
  return normalized;
}

function buildShortfallNotionalAxisModel(minUsdRaw, maxUsdRaw) {
  const minUsdCandidate = toFinitePositiveOrNull(minUsdRaw) ?? 1;
  const maxUsdCandidate = toFinitePositiveOrNull(maxUsdRaw) ?? 10;
  const minUsd = Math.min(minUsdCandidate, maxUsdCandidate);
  const maxUsd = Math.max(maxUsdCandidate, minUsd * 1.000001);
  const minLog = Math.log10(minUsd);
  const maxLog = Math.log10(maxUsd);
  const hasSpan = Number.isFinite(minLog) && Number.isFinite(maxLog) && maxLog > minLog;
  const pivotUsd = SHORTFALL_NOTIONAL_PIVOT_USD;
  const pivotLog = Math.log10(pivotUsd);
  const hasPivot = hasSpan && minUsd < pivotUsd && maxUsd > pivotUsd;
  return {
    minUsd,
    maxUsd,
    minLog,
    maxLog,
    pivotUsd,
    pivotLog,
    hasSpan,
    hasPivot,
    leftShare: SHORTFALL_NOTIONAL_PIVOT_SHARE,
  };
}

function toShortfallNotionalAxisValue(usdRaw, axisModel) {
  if (!axisModel || !axisModel.hasSpan) {
    return null;
  }
  const usd = toFinitePositiveOrNull(usdRaw);
  if (usd === null) {
    return null;
  }
  const clampedUsd = Math.min(axisModel.maxUsd, Math.max(axisModel.minUsd, usd));
  const usdLog = Math.log10(clampedUsd);

  if (!axisModel.hasPivot) {
    const span = axisModel.maxLog - axisModel.minLog;
    if (!(span > 0)) {
      return 0;
    }
    return (usdLog - axisModel.minLog) / span;
  }

  const leftSpan = axisModel.pivotLog - axisModel.minLog;
  const rightSpan = axisModel.maxLog - axisModel.pivotLog;
  if (clampedUsd <= axisModel.pivotUsd) {
    if (!(leftSpan > 0)) {
      return 0;
    }
    return ((usdLog - axisModel.minLog) / leftSpan) * axisModel.leftShare;
  }
  if (!(rightSpan > 0)) {
    return axisModel.leftShare;
  }
  return (
    axisModel.leftShare +
    ((usdLog - axisModel.pivotLog) / rightSpan) * (1 - axisModel.leftShare)
  );
}

function fromShortfallNotionalAxisValue(axisValueRaw, axisModel) {
  if (!axisModel || !axisModel.hasSpan) {
    return null;
  }
  if (!Number.isFinite(axisValueRaw)) {
    return null;
  }
  const axisValue = Math.max(0, Math.min(1, Number(axisValueRaw)));

  if (!axisModel.hasPivot) {
    const usdLog = axisModel.minLog + axisValue * (axisModel.maxLog - axisModel.minLog);
    return 10 ** usdLog;
  }

  if (axisValue <= axisModel.leftShare) {
    const leftRatio =
      axisModel.leftShare > 0 ? axisValue / axisModel.leftShare : 0;
    const usdLog =
      axisModel.minLog + leftRatio * (axisModel.pivotLog - axisModel.minLog);
    return 10 ** usdLog;
  }

  const rightDenominator = 1 - axisModel.leftShare;
  const rightRatio =
    rightDenominator > 0
      ? (axisValue - axisModel.leftShare) / rightDenominator
      : 0;
  const usdLog =
    axisModel.pivotLog + rightRatio * (axisModel.maxLog - axisModel.pivotLog);
  return 10 ** usdLog;
}

function resolveShortfallNotionalAxisModel(snapshot) {
  const buckets = Array.isArray(snapshot?.buckets) ? snapshot.buckets : [];
  const minBucket = buckets[0] ?? null;
  const maxBucket = buckets[buckets.length - 1] ?? null;
  const minUsd = minBucket ? minBucket.lowerUsd : 1;
  const maxUsd = maxBucket ? maxBucket.upperUsd : 10;
  return buildShortfallNotionalAxisModel(minUsd, maxUsd);
}

function toSignedLog10(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const numeric = Number(value);
  const sign = numeric < 0 ? -1 : 1;
  return sign * Math.log10(1 + Math.abs(numeric));
}

function fromSignedLog10(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  const numeric = Number(value);
  const sign = numeric < 0 ? -1 : 1;
  return sign * (10 ** Math.abs(numeric) - 1);
}

function transformShortfallYValue(value, yScale) {
  if (!Number.isFinite(value)) {
    return null;
  }
  if (yScale === "signed_log") {
    return toSignedLog10(Number(value));
  }
  return Number(value);
}

function formatShortfallAxisTick(rawValue) {
  if (!Number.isFinite(rawValue)) {
    return "";
  }
  const value = Number(rawValue);
  const abs = Math.abs(value);
  if (abs >= 1000) {
    return `${Math.round(value)}`;
  }
  if (abs >= 100) {
    return `${value.toFixed(0)}`;
  }
  if (abs >= 10) {
    return `${value.toFixed(1)}`;
  }
  return `${value.toFixed(2)}`;
}

function providerTitle(providerKey) {
  if (providerKey === "lifi") {
    return "LI.FI";
  }
  if (providerKey === "nearintents") {
    return "Near Intents";
  }
  if (providerKey === "thorchain") {
    return "THORChain";
  }
  if (providerKey === "kyberswap") {
    return "KyberSwap";
  }
  return providerKey.charAt(0).toUpperCase() + providerKey.slice(1);
}

function withAlpha(hexColor, alpha) {
  if (typeof hexColor !== "string") {
    return `rgba(255,255,255,${alpha})`;
  }
  const cleaned = hexColor.trim().replace(/^#/, "");
  if (cleaned.length !== 3 && cleaned.length !== 6) {
    return `rgba(255,255,255,${alpha})`;
  }
  const expanded =
    cleaned.length === 3
      ? cleaned
          .split("")
          .map((char) => `${char}${char}`)
          .join("")
      : cleaned;
  const red = parseInt(expanded.slice(0, 2), 16);
  const green = parseInt(expanded.slice(2, 4), 16);
  const blue = parseInt(expanded.slice(4, 6), 16);
  return `rgba(${red},${green},${blue},${alpha})`;
}

async function fetchShortfallSnapshot(viewId) {
  const response = await fetch(
    `/api/shortfall-distribution?view=${encodeURIComponent(viewId)}`,
    { cache: "no-store" },
  );
  if (!response.ok) {
    throw new Error(`Shortfall request failed (${response.status})`);
  }
  return response.json();
}

function ensureCharts() {
  if (!state.charts.shortfall && shortfallQuantileChartEl) {
    state.charts.shortfall = echarts.init(shortfallQuantileChartEl, null, {
      renderer: "canvas",
    });
    state.charts.shortfall.on("datazoom", (event) => {
      const payload = Array.isArray(event?.batch) ? event.batch[0] : event;
      const start = Number(payload?.start);
      const end = Number(payload?.end);
      if (Number.isFinite(start) && Number.isFinite(end)) {
        state.shortfallZoom.startValue = clampPercent(start);
        state.shortfallZoom.endValue = clampPercent(end);
      } else {
        const option = state.charts.shortfall?.getOption();
        const dataZoom = Array.isArray(option?.dataZoom) ? option.dataZoom[0] : null;
        state.shortfallZoom.startValue = clampPercent(Number(dataZoom?.start));
        state.shortfallZoom.endValue = clampPercent(Number(dataZoom?.end));
      }
      updateShortfallYAxisFromZoom();
    });
    state.charts.shortfall.on("legendselectchanged", (event) => {
      const selected = event?.selected;
      state.shortfallLegendSelected =
        selected && typeof selected === "object" ? { ...selected } : {};
      updateShortfallYAxisFromZoom();
    });
  }
}

function buildTableRows(shortfallSnapshot) {
  providersBody.innerHTML = "";
  const providers = Array.isArray(shortfallSnapshot?.providers) ? shortfallSnapshot.providers : [];
  const rows = [...providers].sort(
    (a, b) => PROVIDER_ORDER.indexOf(a.providerKey) - PROVIDER_ORDER.indexOf(b.providerKey),
  );

  let erroredProviders = 0;
  let missingProviders = 0;

  for (const provider of rows) {
    const tr = document.createElement("tr");
    let stateClass = "state-ok";
    let stateLabel = "ok";
    if (provider.error) {
      stateClass = "state-warn";
      stateLabel = "error";
      erroredProviders += 1;
    } else if (!provider.found) {
      stateClass = "state-warn";
      stateLabel = "missing";
      missingProviders += 1;
    }

    const pricedMatchRate = Number.isFinite(provider.pricedMatchRatePct)
      ? formatRate(provider.pricedMatchRatePct)
      : "-";

    tr.innerHTML = `
      <td>${providerTitle(provider.providerKey)}</td>
      <td>${formatInt(provider.matchedTrades)}</td>
      <td>${formatInt(provider.pricedTrades)} (${pricedMatchRate})</td>
      <td>${formatUsd(provider.analyzedVolumeUsd)}</td>
      <td>${formatAnalyzedRange(provider.analyzedRangeStartAt, provider.analyzedRangeEndAt)}</td>
      <td><span class="state-tag ${stateClass}">${stateLabel}</span></td>
    `;
    providersBody.appendChild(tr);
  }

  const totalSwaps = rows.reduce((sum, provider) => {
    const value = Number(provider.matchedTrades);
    return Number.isFinite(value) && value > 0 ? sum + value : sum;
  }, 0);
  const totalVolumeUsd = rows.reduce(
    (sum, provider) => {
      const value = Number(provider.analyzedVolumeUsd);
      return Number.isFinite(value) && value > 0 ? sum + value : sum;
    },
    0,
  );

  const startTimes = rows
    .map((provider) => {
      if (!provider.analyzedRangeStartAt) {
        return Number.NaN;
      }
      return new Date(provider.analyzedRangeStartAt).getTime();
    })
    .filter((value) => Number.isFinite(value));
  const endTimes = rows
    .map((provider) => {
      if (!provider.analyzedRangeEndAt) {
        return Number.NaN;
      }
      return new Date(provider.analyzedRangeEndAt).getTime();
    })
    .filter((value) => Number.isFinite(value));
  const startAt = startTimes.length > 0 ? new Date(Math.min(...startTimes)).toISOString() : null;
  const endAt = endTimes.length > 0 ? new Date(Math.max(...endTimes)).toISOString() : null;
  const rangeLabel = formatAnalyzedRange(startAt, endAt);

  statusText.textContent = `${formatInt(totalSwaps)} swaps | ${formatUsd(totalVolumeUsd)} | ${rangeLabel}`;
  if (erroredProviders > 0) {
    statusText.textContent += ` | ${erroredProviders} provider errors`;
  } else if (missingProviders > 0) {
    statusText.textContent += ` | ${missingProviders} providers missing data`;
  }
}

function createBandRenderItem(baseColor) {
  return (params, api) => {
    const xStart = api.coord([api.value(0), 0])[0];
    const xEnd = api.coord([api.value(1), 0])[0];
    const yLow = api.coord([api.value(0), api.value(2)])[1];
    const yHigh = api.coord([api.value(0), api.value(3)])[1];

    const shape = echarts.graphic.clipRectByRect(
      {
        x: Math.min(xStart, xEnd),
        y: Math.min(yLow, yHigh),
        width: Math.abs(xEnd - xStart),
        height: Math.abs(yHigh - yLow),
      },
      params.coordSys,
    );

    if (!shape) {
      return null;
    }

    const opacity = Number(api.value(4));
    const normalizedOpacity = Number.isFinite(opacity)
      ? Math.max(0.02, Math.min(0.95, opacity))
      : 0.12;

    return {
      type: "rect",
      shape,
      style: {
        fill: withAlpha(baseColor, normalizedOpacity),
      },
      silent: true,
    };
  };
}

function computeBandOpacity(count, minSampleCount, isInnerBand) {
  const denominator = minSampleCount * 6;
  const confidence = denominator <= 0 ? 1 : Math.min(1, count / denominator);
  if (isInnerBand) {
    return 0.22 + confidence * 0.22;
  }
  return 0.08 + confidence * 0.15;
}

function clampPercent(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  return Math.min(100, Math.max(0, Number(value)));
}

function overlapRange(aStart, aEnd, bStart, bEnd) {
  return aStart <= bEnd && aEnd >= bStart;
}

function isProviderSelectedForShortfall(providerKey, selectedLegend) {
  const legendName = providerTitle(providerKey);
  if (!selectedLegend || typeof selectedLegend !== "object") {
    return true;
  }
  return selectedLegend[legendName] !== false;
}

function collectVisibleShortfallValues(
  snapshot,
  visibleStartAxis,
  visibleEndAxis,
  selectedLegend,
  showBand,
  axisModel,
) {
  const buckets = Array.isArray(snapshot?.buckets) ? snapshot.buckets : [];
  const providers = Array.isArray(snapshot?.providers) ? snapshot.providers : [];
  const bucketByIndex = new Map(buckets.map((bucket) => [bucket.bucketIndex, bucket]));
  const values = [];

  for (const provider of providers) {
    if (!isProviderSelectedForShortfall(provider.providerKey, selectedLegend)) {
      continue;
    }
    for (const bucketQuantiles of provider.buckets ?? []) {
      const bucket = bucketByIndex.get(bucketQuantiles.bucketIndex);
      if (!bucket) {
        continue;
      }
      const lowerAxis = toShortfallNotionalAxisValue(bucket.lowerUsd, axisModel);
      const upperAxis = toShortfallNotionalAxisValue(bucket.upperUsd, axisModel);
      if (!Number.isFinite(lowerAxis) || !Number.isFinite(upperAxis)) {
        continue;
      }
      if (!overlapRange(lowerAxis, upperAxis, visibleStartAxis, visibleEndAxis)) {
        continue;
      }
      const quantiles = showBand
        ? [bucketQuantiles.q25, bucketQuantiles.q50, bucketQuantiles.q75]
        : [bucketQuantiles.q50];
      for (const quantile of quantiles) {
        if (Number.isFinite(quantile)) {
          values.push(Number(quantile));
        }
      }
    }
  }

  return values;
}

function chooseShortfallStep(span) {
  if (span <= 20) {
    return 2;
  }
  if (span <= 50) {
    return 5;
  }
  if (span <= 100) {
    return 10;
  }
  if (span <= 250) {
    return 25;
  }
  if (span <= 500) {
    return 50;
  }
  if (span <= 1000) {
    return 100;
  }
  if (span <= 2500) {
    return 250;
  }
  return 500;
}

function computeShortfallYAxisRange(
  snapshot,
  visibleStartAxis,
  visibleEndAxis,
  selectedLegend,
  yScale,
  showBand,
  axisModel,
) {
  const rawVisibleValues = collectVisibleShortfallValues(
    snapshot,
    visibleStartAxis,
    visibleEndAxis,
    selectedLegend,
    showBand,
    axisModel,
  );
  const visibleValues =
    yScale === "signed_log"
      ? rawVisibleValues
          .map((value) => toSignedLog10(value))
          .filter((value) => Number.isFinite(value))
      : rawVisibleValues;

  if (visibleValues.length === 0) {
    return {
      min: yScale === "signed_log" ? -2 : -100,
      max: yScale === "signed_log" ? 2 : 100,
    };
  }

  const minValue = Math.min(...visibleValues);
  const maxValue = Math.max(...visibleValues);
  if (!Number.isFinite(minValue) || !Number.isFinite(maxValue)) {
    return {
      min: yScale === "signed_log" ? -2 : -100,
      max: yScale === "signed_log" ? 2 : 100,
    };
  }

  if (yScale === "signed_log") {
    // Keep headroom tight in signed-log mode; coarse rounding creates too much top whitespace.
    const span = Math.max(0.05, maxValue - minValue);
    const topPadding = Math.max(0.01, span * 0.015);
    const bottomPadding = Math.max(0.01, span * 0.03);
    let min = minValue - bottomPadding;
    let max = maxValue + topPadding;
    if (min === max) {
      min -= 0.05;
      max += 0.05;
    }
    return { min, max };
  }

  const span = Math.max(1, maxValue - minValue);
  const padding = Math.max(2, span * 0.08);
  const paddedMin = minValue - padding;
  const paddedMax = maxValue + padding;
  const step = chooseShortfallStep(paddedMax - paddedMin);
  let min = Math.floor(paddedMin / step) * step;
  let max = Math.ceil(paddedMax / step) * step;
  if (min === max) {
    min -= step;
    max += step;
  }

  return { min, max };
}

function updateShortfallYAxisFromZoom() {
  const chart = state.charts.shortfall;
  const snapshot = state.shortfallSnapshot;
  if (!chart || !snapshot) {
    return;
  }

  const axisModel = resolveShortfallNotionalAxisModel(snapshot);
  const xMinAxis = 0;
  const xMaxAxis = 1;

  let startPercent = clampPercent(state.shortfallZoom.startValue);
  let endPercent = clampPercent(state.shortfallZoom.endValue);
  if (startPercent === null) {
    startPercent = 0;
  }
  if (endPercent === null) {
    endPercent = 100;
  }
  if (startPercent >= endPercent) {
    startPercent = 0;
    endPercent = 100;
  }
  const span = Math.max(0, xMaxAxis - xMinAxis);
  const visibleStartAxis = xMinAxis + (span * startPercent) / 100;
  const visibleEndAxis = xMinAxis + (span * endPercent) / 100;

  const yRange = computeShortfallYAxisRange(
    snapshot,
    visibleStartAxis,
    visibleEndAxis,
    state.shortfallLegendSelected,
    state.shortfallYAxisScale,
    state.shortfallShowBand,
    axisModel,
  );
  chart.setOption(
    {
      yAxis: {
        min: yRange.min,
        max: yRange.max,
      },
    },
    false,
  );
}

function shortfallTooltipFormatter(params) {
  const rows = Array.isArray(params) ? params : [params];
  const payloads = rows
    .map((row) => row?.data)
    .filter((data) => data && typeof data === "object")
    .sort((left, right) => {
      const leftMedian = Number(left.q50);
      const rightMedian = Number(right.q50);
      const leftFinite = Number.isFinite(leftMedian);
      const rightFinite = Number.isFinite(rightMedian);
      if (leftFinite && rightFinite && leftMedian !== rightMedian) {
        return leftMedian - rightMedian;
      }
      if (leftFinite && !rightFinite) {
        return -1;
      }
      if (!leftFinite && rightFinite) {
        return 1;
      }
      const leftIndex = PROVIDER_ORDER.indexOf(left.providerKey);
      const rightIndex = PROVIDER_ORDER.indexOf(right.providerKey);
      return leftIndex - rightIndex;
    });

  if (payloads.length === 0) {
    return "No quantile data";
  }

  const first = payloads[0];
  const sections = payloads.map((item) => {
    const color = PROVIDER_COLORS[item.providerKey] ?? "#9fb4c8";
    return `
      <div style="margin-top:6px; line-height:1.4;">
        <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${color};margin-right:6px;"></span>
        <strong>${providerTitle(item.providerKey)}</strong>
        <span style="color:#9fb4c8;">N ${formatInt(item.count)}</span><br />
        Median: ${formatBps(item.q50)}<br />
        Q25/Q75: ${formatBps(item.q25)} / ${formatBps(item.q75)}<br />
        Q05/Q95: ${formatBps(item.q05)} / ${formatBps(item.q95)}
      </div>
    `;
  });

  return `
    <div style="font-family:Space Grotesk, sans-serif;">
      <div style="margin-bottom:4px;"><strong>Notional ${formatUsdRange(first.lowerUsd, first.upperUsd)}</strong></div>
      ${sections.join("")}
    </div>
  `;
}

function buildShortfallSeries(snapshot, minSampleCount, yScale, showBand, axisModel) {
  const buckets = Array.isArray(snapshot?.buckets) ? snapshot.buckets : [];
  const providers = Array.isArray(snapshot?.providers) ? snapshot.providers : [];
  const bucketByIndex = new Map(buckets.map((bucket) => [bucket.bucketIndex, bucket]));
  const providersSorted = [...providers].sort(
    (a, b) => PROVIDER_ORDER.indexOf(a.providerKey) - PROVIDER_ORDER.indexOf(b.providerKey),
  );

  const legendNames = [];
  const series = [];

  for (const provider of providersSorted) {
    const providerName = providerTitle(provider.providerKey);
    const color = PROVIDER_COLORS[provider.providerKey] ?? "#9fb4c8";
    const innerBandData = [];
    const medianLineData = [];

    for (const bucketQuantiles of provider.buckets ?? []) {
      const bucket = bucketByIndex.get(bucketQuantiles.bucketIndex);
      if (!bucket) {
        continue;
      }

      const payload = {
        providerKey: provider.providerKey,
        bucketIndex: bucket.bucketIndex,
        bucketLabel: bucket.label,
        lowerUsd: bucket.lowerUsd,
        upperUsd: bucket.upperUsd,
        count: bucketQuantiles.count,
        q05: bucketQuantiles.q05,
        q25: bucketQuantiles.q25,
        q50: bucketQuantiles.q50,
        q75: bucketQuantiles.q75,
        q95: bucketQuantiles.q95,
      };

      if (
        showBand &&
        Number.isFinite(bucketQuantiles.q25) &&
        Number.isFinite(bucketQuantiles.q75)
      ) {
        const q25Value = transformShortfallYValue(bucketQuantiles.q25, yScale);
        const q75Value = transformShortfallYValue(bucketQuantiles.q75, yScale);
        const lowerAxisValue = toShortfallNotionalAxisValue(bucket.lowerUsd, axisModel);
        const upperAxisValue = toShortfallNotionalAxisValue(bucket.upperUsd, axisModel);
        if (
          !Number.isFinite(lowerAxisValue) ||
          !Number.isFinite(upperAxisValue) ||
          !Number.isFinite(q25Value) ||
          !Number.isFinite(q75Value)
        ) {
          continue;
        }
        innerBandData.push({
          value: [
            lowerAxisValue,
            upperAxisValue,
            q25Value,
            q75Value,
            computeBandOpacity(bucketQuantiles.count, minSampleCount, true),
          ],
          ...payload,
        });
      }

      if (Number.isFinite(bucketQuantiles.q50)) {
        const q50Value = transformShortfallYValue(bucketQuantiles.q50, yScale);
        const centerAxisValue = toShortfallNotionalAxisValue(bucket.centerUsd, axisModel);
        if (!Number.isFinite(centerAxisValue) || !Number.isFinite(q50Value)) {
          continue;
        }
        medianLineData.push({
          value: [centerAxisValue, q50Value],
          ...payload,
        });
      }
    }

    if (innerBandData.length === 0 && medianLineData.length === 0) {
      continue;
    }

    legendNames.push(providerName);

    if (innerBandData.length > 0) {
      series.push({
        name: providerName,
        type: "custom",
        color,
        itemStyle: { color },
        renderItem: createBandRenderItem(color),
        encode: { x: [0, 1], y: [2, 3] },
        data: innerBandData,
        silent: true,
        tooltip: { show: false },
        z: 2,
        animation: false,
      });
    }

    if (medianLineData.length > 0) {
      series.push({
        name: providerName,
        type: "line",
        color,
        smooth: false,
        connectNulls: false,
        showSymbol: true,
        symbolSize: 6,
        lineStyle: {
          width: 2,
          color,
        },
        itemStyle: {
          color,
          borderColor: withAlpha("#ffffff", 0.45),
          borderWidth: 1,
        },
        data: medianLineData,
        z: 4,
      });
    }
  }

  return {
    legendNames,
    series,
  };
}

function renderShortfallSection(snapshot) {
  state.shortfallSnapshot = snapshot;
  const yScale = state.shortfallYAxisScale === "signed_log" ? "signed_log" : "linear";
  const showBand = Boolean(state.shortfallShowBand);
  const bucketConfig = snapshot?.bucketConfig ?? null;
  const buckets = Array.isArray(snapshot?.buckets) ? snapshot.buckets : [];
  const minSampleCount = Number.isFinite(bucketConfig?.minSampleCount)
    ? Number(bucketConfig.minSampleCount)
    : 10;

  if (!state.charts.shortfall) {
    return;
  }

  const axisModel = resolveShortfallNotionalAxisModel(snapshot);
  const { legendNames, series } = buildShortfallSeries(
    snapshot,
    minSampleCount,
    yScale,
    showBand,
    axisModel,
  );
  const hasSeries = series.length > 0;

  const nextLegendSelected = {};
  for (const legendName of legendNames) {
    const previouslySelected = state.shortfallLegendSelected?.[legendName];
    nextLegendSelected[legendName] = previouslySelected !== false;
  }
  state.shortfallLegendSelected = nextLegendSelected;

  const xMinAxis = 0;
  const xMaxAxis = 1;

  let zoomStartPercent = clampPercent(state.shortfallZoom.startValue);
  let zoomEndPercent = clampPercent(state.shortfallZoom.endValue);
  if (zoomStartPercent === null) {
    zoomStartPercent = 0;
  }
  if (zoomEndPercent === null) {
    zoomEndPercent = 100;
  }
  if (zoomStartPercent >= zoomEndPercent) {
    zoomStartPercent = 0;
    zoomEndPercent = 100;
  }
  const span = Math.max(0, xMaxAxis - xMinAxis);
  const visibleStartAxis = xMinAxis + (span * zoomStartPercent) / 100;
  const visibleEndAxis = xMinAxis + (span * zoomEndPercent) / 100;
  const yRange = computeShortfallYAxisRange(
    snapshot,
    visibleStartAxis,
    visibleEndAxis,
    state.shortfallLegendSelected,
    yScale,
    showBand,
    axisModel,
  );

  state.charts.shortfall.clear();
  state.charts.shortfall.setOption(
    {
      animationDuration: 300,
      backgroundColor: "transparent",
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "line",
          snap: true,
        },
        confine: true,
        backgroundColor: "rgba(11, 21, 31, 0.96)",
        borderColor: "rgba(255,255,255,0.12)",
        textStyle: { color: "#e8f1fb", fontSize: 12 },
        formatter: shortfallTooltipFormatter,
      },
      legend: {
        top: 0,
        data: legendNames,
        icon: "roundRect",
        itemWidth: 14,
        itemHeight: 10,
        selected: state.shortfallLegendSelected,
        selectedMode: true,
        textStyle: { color: "#9fb4c8", fontFamily: "Space Grotesk" },
      },
      grid: { left: 58, right: 24, top: 46, bottom: 104 },
      xAxis: {
        type: "value",
        min: xMinAxis,
        max: xMaxAxis,
        name: "Input Notional (USD, weighted log scale)",
        nameLocation: "middle",
        nameGap: 60,
        nameTextStyle: { color: "#9fb4c8" },
        axisLabel: {
          color: "#9fb4c8",
          formatter: (value) => {
            const usdValue = fromShortfallNotionalAxisValue(Number(value), axisModel);
            return usdValue !== null ? `$${formatUsdTick(usdValue)}` : "";
          },
        },
        axisLine: { lineStyle: { color: "rgba(255,255,255,0.2)" } },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.08)" } },
      },
      yAxis: {
        type: "value",
        min: yRange.min,
        max: yRange.max,
        name:
          yScale === "signed_log"
            ? "Implementation Shortfall (bps, log)"
            : "Implementation Shortfall (bps)",
        nameLocation: "middle",
        nameGap: 48,
        nameTextStyle: { color: "#9fb4c8" },
        axisLabel: {
          color: "#9fb4c8",
          formatter: (value) => {
            if (yScale === "signed_log") {
              const rawValue = fromSignedLog10(Number(value));
              return rawValue !== null ? formatShortfallAxisTick(rawValue) : "";
            }
            return formatShortfallAxisTick(Number(value));
          },
        },
        axisLine: { lineStyle: { color: "rgba(255,255,255,0.2)" } },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.08)" } },
      },
      dataZoom: [
        {
          type: "slider",
          xAxisIndex: 0,
          filterMode: "none",
          showDataShadow: false,
          bottom: 26,
          height: 18,
          borderColor: "rgba(255,255,255,0.16)",
          backgroundColor: "rgba(255,255,255,0.04)",
          fillerColor: "rgba(94,194,255,0.22)",
          handleStyle: {
            color: "#5ec2ff",
            borderColor: "rgba(255,255,255,0.5)",
          },
          moveHandleStyle: {
            color: "rgba(159,180,200,0.6)",
          },
          textStyle: {
            color: "#9fb4c8",
            fontFamily: "JetBrains Mono",
            fontSize: 10,
          },
          labelFormatter: (value) => {
            const usdValue = fromShortfallNotionalAxisValue(Number(value), axisModel);
            return usdValue !== null ? `$${formatUsdTick(usdValue)}` : "";
          },
          start: zoomStartPercent,
          end: zoomEndPercent,
        },
        {
          type: "inside",
          xAxisIndex: 0,
          filterMode: "none",
          start: zoomStartPercent,
          end: zoomEndPercent,
          zoomOnMouseWheel: true,
          moveOnMouseMove: true,
          moveOnMouseWheel: false,
        },
      ],
      series,
      graphic: hasSeries
        ? []
        : {
            type: "text",
            left: "center",
            top: "middle",
            style: {
              text: "No bucket has sufficient observations for quantiles",
              fill: "#9fb4c8",
              font: "13px Space Grotesk",
            },
          },
    },
    true,
  );
}

async function refreshDashboard() {
  try {
    refreshBtn.disabled = true;
    statusText.textContent = "Loading...";

    const shortfallSnapshot = await fetchShortfallSnapshot(state.activeShortfallView);

    ensureCharts();
    buildTableRows(shortfallSnapshot);
    renderShortfallSection(shortfallSnapshot);
  } catch (error) {
    statusText.textContent = error instanceof Error ? error.message : String(error);
  } finally {
    refreshBtn.disabled = false;
  }
}

window.addEventListener("resize", () => {
  if (state.charts.shortfall) {
    state.charts.shortfall.resize();
  }
});

if (shortfallViewSelect) {
  shortfallViewSelect.value = state.activeShortfallView;
  shortfallViewSelect.addEventListener("change", () => {
    state.activeShortfallView = shortfallViewSelect.value;
    refreshDashboard();
  });
}

if (shortfallYScaleSelect) {
  shortfallYScaleSelect.value = state.shortfallYAxisScale;
  shortfallYScaleSelect.addEventListener("change", () => {
    state.shortfallYAxisScale =
      shortfallYScaleSelect.value === "signed_log" ? "signed_log" : "linear";
    if (state.shortfallSnapshot) {
      ensureCharts();
      renderShortfallSection(state.shortfallSnapshot);
    }
  });
}

if (shortfallBandToggle instanceof HTMLInputElement) {
  shortfallBandToggle.checked = state.shortfallShowBand;
  shortfallBandToggle.addEventListener("change", () => {
    state.shortfallShowBand = shortfallBandToggle.checked;
    if (state.shortfallSnapshot) {
      ensureCharts();
      renderShortfallSection(state.shortfallSnapshot);
    }
  });
}

refreshBtn.addEventListener("click", () => {
  refreshDashboard();
});

refreshDashboard();
setInterval(refreshDashboard, 60_000);
