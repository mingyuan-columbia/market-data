Yes — **Plotly can automatically adjust the y-axis when you zoom/pan**, but there are a couple of **important gotchas**, especially in **Streamlit**.

Below is the **correct mental model + exact settings** that work.

---

## 1️⃣ How Plotly auto-range is *supposed* to work

By default, Plotly **does auto-range the y-axis when you zoom the x-axis**.

This happens **client-side** and is very fast.

### Minimum requirement

You must **not lock the y-axis**.

```python
fig.update_yaxes(autorange=True)
```

or simply **don’t specify a fixed range at all**.

---

## 2️⃣ The #1 reason it *doesn’t* work (very common)

### ❌ You set a fixed y-range somewhere

Any of these will **disable auto-range on zoom**:

```python
fig.update_yaxes(range=[ymin, ymax])
```

or

```python
fig.update_layout(yaxis=dict(range=[...]))
```

Once a range is set, Plotly assumes:

> “User wants this range forever.”

✅ **Fix**: remove the range, or explicitly re-enable autorange.

---

## 3️⃣ The correct settings (safe defaults)

Use this pattern:

```python
fig.update_layout(
    xaxis=dict(
        rangeslider=dict(visible=False),
        fixedrange=False
    ),
    yaxis=dict(
        autorange=True,
        fixedrange=False
    )
)
```

This allows:

* zoom
* pan
* auto y-rescale

---

## 4️⃣ WebGL (`Scattergl`) caveat (important)

You are using `Scattergl` (WebGL), which is correct for performance.

Good news:

* **Auto y-range works with `Scattergl`**
* But it only considers **visible points**

Bad news:

* If you downsample aggressively, the y-range reflects the **downsampled view**, not raw ticks
  (this is expected and usually desirable)

---

## 5️⃣ Streamlit-specific gotcha (very important)

In **Streamlit**, this breaks auto-range:

> ❌ Re-creating the figure on every rerun

When Streamlit reruns:

* the zoom state is lost
* Plotly resets axes
* autorange may look “broken”

### ✅ Fix: preserve zoom state using `uirevision`

```python
fig.update_layout(uirevision="keep")
```

This tells Plotly:

> “Don’t reset zoom/pan when the figure object updates.”

**This is critical** for interactive exploration.

---

## 6️⃣ Full minimal example (works)

```python
import plotly.graph_objects as go

fig = go.Figure()

fig.add_trace(go.Scattergl(
    x=q["quote_ts"],
    y=q["mid"],
    mode="lines",
    name="Mid"
))

fig.add_trace(go.Scattergl(
    x=q["quote_ts"],
    y=q["bid_px"],
    mode="lines",
    name="Bid"
))

fig.add_trace(go.Scattergl(
    x=q["quote_ts"],
    y=q["ask_px"],
    mode="lines",
    name="Ask"
))

fig.update_layout(
    height=600,
    uirevision="keep",
    xaxis=dict(fixedrange=False),
    yaxis=dict(autorange=True, fixedrange=False)
)
```

Now:

* zoom x → y auto-rescales
* pan → y updates
* Streamlit reruns → zoom preserved

---

## 7️⃣ Optional: manual “reset y-axis” button

Users sometimes want a reset.

Add Plotly modebar button:

```python
fig.update_layout(
    modebar_add=["resetScale2d"]
)
```

Or instruct users to double-click the plot.

---

## 8️⃣ If you want *conditional* y-scaling (advanced)

For example:

* tight y-range when zoomed
* padded y-range when fully zoomed out

That requires:

* listening to `plotly_relayout` events
* recomputing y-limits server-side

This is possible in Dash, **not easily in Streamlit**.

---

## TL;DR

✅ **Yes, Plotly supports auto y-axis rescaling on zoom**
✅ Works with `Scattergl`
❌ Breaks if you set `yaxis.range`
❌ Breaks in Streamlit without `uirevision`

### Must-have settings:

```python
yaxis=dict(autorange=True)
uirevision="keep"
```

You’re *very close* — the behavior you’re seeing is because **`yaxis.fixedrange=True` locks the y-axis range** for zoom/pan interactions, and in practice it also prevents Plotly from recomputing the y-range on x-zoom in many cases (especially when you’ve already set an initial axis range or the plot has been interacted with). So **autorange won’t “react”** the way you expect.

### The reliable fix

Instead of `fixedrange=True`, use `fixedrange=False` and **remove the y-zoom tools** (so users can’t manually y-zoom), while still letting Plotly auto-rescale y when x changes.

#### Option A (recommended): allow autorange + keep y non-zoomable via modebar

```python
yaxis_config = dict(
    autorange=True,
    fixedrange=False,   # allow Plotly to change y-range
)

fig.update_layout(
    xaxis=xaxis_config,
    yaxis=yaxis_config,
    uirevision="keep",
    dragmode="zoom",  # or "pan"
    modebar_remove=["zoomIn2d", "zoomOut2d", "autoScale2d"],  # optional
)
```

Then you control “x-only zoom” using **`dragmode="zoom"` plus y-axis behavior**. If you truly want “x-only box zoom”, keep reading (Option C).

---

## Why your current setup keeps y fixed

* `fixedrange=True` is documented as “disable zoom” on that axis.
* In many Plotly interactions, disabling zoom also effectively disables **auto relayout of that axis** during interaction. So y remains stuck.

---

## Two better patterns depending on what you want

### Option B: “x-only zoom” AND y auto-rescales (best for time-series)

This is what most people want, and it works well in Streamlit:

```python
fig.update_layout(
    xaxis=dict(rangeslider=dict(visible=False), fixedrange=False),
    yaxis=dict(autorange=True, fixedrange=False),
    uirevision="keep",
)

# lock manual y-zoom by making the drag zoom only affect x
fig.update_layout(dragmode="zoom")
```

In practice, users can still *technically* zoom y via axis drag in the UI, but most won’t. If you need to fully prevent that, use Option C.

---

### Option C: Truly prevent y-zoom, but still allow y auto-rescale on x-zoom

Plotly’s built-in “fixedrange” is too blunt. The workaround is:

1. keep `yaxis.fixedrange=False` so autorange can work
2. **force zooming to x only** by setting a fixed y-range on each relayout event (Dash supports this easily; Streamlit is limited)

In Streamlit, the best you can do without event callbacks is:

* keep `fixedrange=False` (so y can auto-rescale)
* remove y-zoom UI affordances as much as possible (modebar + user guidance)

If you were using Dash, you’d listen to `relayoutData` and enforce `yaxis.range` each time.

---

## One more thing to check: are you accidentally setting y-range elsewhere?

If anywhere you do:

```python
fig.update_yaxes(range=[...])
```

or

```python
fig.update_layout(yaxis=dict(range=[...]))
```

then autorange won’t kick in. Search your codebase for `yaxis.range` or `update_yaxes(range=`.

---

## A known good config for Streamlit (copy/paste)

This should give you:

* x zoom/pan
* y auto-rescale on zoom
* stable interaction across reruns

```python
xaxis_config = dict(
    rangeslider=dict(visible=False),
    fixedrange=False,
)

yaxis_config = dict(
    autorange=True,
    fixedrange=False,   # KEY CHANGE
)

fig.update_layout(
    title=title,
    xaxis_title="Time (NY)",
    yaxis_title="Price ($)",
    hovermode="x unified",
    height=600,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    xaxis=xaxis_config,
    yaxis=yaxis_config,
    uirevision="keep",
    dragmode="zoom",
)
```

