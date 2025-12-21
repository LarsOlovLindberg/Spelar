(() => {
  const STORAGE_KEYS = {
    difficulty: "spelar:difficulty",
    bestReactionMs: "spelar:bestReactionMs",
    bestMathScore: "spelar:bestMathScore",
    bestGuessStreak: "spelar:bestGuessStreak",
    bestSnakeLen: "spelar:bestSnakeLen",
    bestRaceScore: "spelar:bestRaceScore",
    bestInvadersScore: "spelar:bestInvadersScore",
  };

  const clampInt = (value, min, max) => Math.max(min, Math.min(max, Number.parseInt(String(value), 10)));

  const loadInt = (key, fallback) => {
    try {
      const raw = localStorage.getItem(key);
      if (raw == null) return fallback;
      const parsed = Number.parseInt(raw, 10);
      return Number.isFinite(parsed) ? parsed : fallback;
    } catch {
      return fallback;
    }
  };

  const saveInt = (key, value) => {
    try {
      localStorage.setItem(key, String(value));
    } catch {
      // ignore
    }
  };

  const el = (tag, attrs = {}, children = []) => {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "text") node.textContent = v;
      else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2).toLowerCase(), v);
      else if (v != null) node.setAttribute(k, String(v));
    }
    for (const child of children) {
      if (child == null) continue;
      node.appendChild(typeof child === "string" ? document.createTextNode(child) : child);
    }
    return node;
  };

  const gameRoot = document.getElementById("gameRoot");
  const difficultyInput = document.getElementById("difficulty");
  const difficultyValue = document.getElementById("difficultyValue");

  let difficulty = clampInt(loadInt(STORAGE_KEYS.difficulty, 3), 1, 5);
  difficultyInput.value = String(difficulty);
  difficultyValue.textContent = String(difficulty);

  difficultyInput.addEventListener("input", () => {
    difficulty = clampInt(difficultyInput.value, 1, 5);
    difficultyValue.textContent = String(difficulty);
    saveInt(STORAGE_KEYS.difficulty, difficulty);

    const current = document.querySelector('.nav__btn[aria-current="page"]');
    if (current) renderGame(current.dataset.game);
  });

  const difficultyLabel = () => {
    switch (difficulty) {
      case 1: return "Mycket lätt";
      case 2: return "Lätt";
      case 3: return "Normal";
      case 4: return "Svår";
      case 5: return "Mycket svår";
      default: return "";
    }
  };

  const setNav = (gameId) => {
    document.querySelectorAll(".nav__btn").forEach((b) => {
      if (b.dataset.game === gameId) b.setAttribute("aria-current", "page");
      else b.removeAttribute("aria-current");
    });
  };

  const clearRoot = () => {
    while (gameRoot.firstChild) gameRoot.removeChild(gameRoot.firstChild);
  };

  const makeCanvas = (w, h) => {
    const c = document.createElement("canvas");
    c.width = w;
    c.height = h;
    c.className = "canvas";
    c.setAttribute("role", "img");
    return c;
  };

  let currentCleanup = null;

  const gameGuess = () => {
    let target = 0;
    let max = 20;
    let tries = 0;
    let streak = 0;

    const pickMax = () => {
      switch (difficulty) {
        case 1: return 20;
        case 2: return 50;
        case 3: return 100;
        case 4: return 250;
        case 5: return 500;
        default: return 100;
      }
    };

    const startNew = () => {
      max = pickMax();
      target = 1 + Math.floor(Math.random() * max);
      tries = 0;
    };

    let bestStreak = loadInt(STORAGE_KEYS.bestGuessStreak, 0);

    const view = () => {
      const title = el("h2", { class: "h2", text: "Gissa talet" });
      const desc = el("p", { class: "p", text: `Jag tänker på ett tal mellan 1 och ${max}. Svårighet: ${difficultyLabel()}.` });

      const input = el("input", { class: "input", type: "number", min: "1", max: String(max), placeholder: "Din gissning" });
      const msg = el("div", { class: "note", text: "Skriv en gissning och tryck Gissa." });
      const streakStrong = el("strong", { text: String(streak) });
      const bestStrong = el("strong", { text: String(bestStreak) });
      const badgeRow = el("div", { class: "row" }, [
        el("span", { class: "badge" }, ["Streak: ", streakStrong]),
        el("span", { class: "badge" }, ["Bästa streak: ", bestStrong]),
      ]);

      const onGuess = () => {
        const n = Number.parseInt(input.value, 10);
        if (!Number.isFinite(n) || n < 1 || n > max) {
          msg.className = "note note--bad";
          msg.textContent = `Skriv ett heltal mellan 1 och ${max}.`;
          return;
        }

        tries += 1;
        if (n === target) {
          streak += 1;
          streakStrong.textContent = String(streak);
          msg.className = "note note--ok";
          msg.textContent = `Rätt! (${target}) Du klarade det på ${tries} försök. Ny runda!`;
          if (streak > bestStreak) {
            bestStreak = streak;
            bestStrong.textContent = String(bestStreak);
            saveInt(STORAGE_KEYS.bestGuessStreak, bestStreak);
          }
          startNew();
          desc.textContent = `Jag tänker på ett tal mellan 1 och ${max}. Svårighet: ${difficultyLabel()}.`;
          input.value = "";
          setTimeout(() => input.focus(), 0);
          return;
        }

        msg.className = "note";
        msg.textContent = n < target ? "För lågt — prova igen." : "För högt — prova igen.";
      };

      const onResetStreak = () => {
        streak = 0;
        streakStrong.textContent = "0";
        msg.className = "note";
        msg.textContent = "Streak nollad. Kör!";
      };

      const btnGuess = el("button", { class: "btn btn--primary", onClick: onGuess, text: "Gissa" });
      const btnNew = el("button", { class: "btn", onClick: () => { startNew(); msg.className = "note"; msg.textContent = "Ny runda startad."; desc.textContent = `Jag tänker på ett tal mellan 1 och ${max}. Svårighet: ${difficultyLabel()}.`; input.value = ""; input.focus(); }, text: "Ny runda" });
      const btnReset = el("button", { class: "btn btn--danger", onClick: onResetStreak, text: "Nolla streak" });

      const row = el("div", { class: "row" }, [input, btnGuess, btnNew, btnReset]);

      const wrapper = el("div", {}, [title, desc, badgeRow, row, msg]);
      input.addEventListener("keydown", (e) => { if (e.key === "Enter") onGuess(); });

      return wrapper;
    };

    startNew();
    return view();
  };

  const gameReaction = () => {
    let best = loadInt(STORAGE_KEYS.bestReactionMs, 0);

    let state = "idle"; // idle | waiting | go | done
    let goAt = 0;
    let timer = null;

    const thresholds = () => {
      switch (difficulty) {
        case 1: return { okMs: 650, greatMs: 450 };
        case 2: return { okMs: 550, greatMs: 380 };
        case 3: return { okMs: 480, greatMs: 320 };
        case 4: return { okMs: 420, greatMs: 280 };
        case 5: return { okMs: 360, greatMs: 240 };
        default: return { okMs: 480, greatMs: 320 };
      }
    };

    const cleanup = () => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
    };

    const view = () => {
      const title = el("h2", { class: "h2", text: "Reaktion" });
      const desc = el("p", { class: "p", text: `Tryck START, vänta tills rutan blir grön och klicka så snabbt du kan. Svårighet: ${difficultyLabel()}.` });

      const bestStrong = el("strong", { text: best > 0 ? `${best} ms` : "—" });
      const badgeRow = el("div", { class: "row" }, [
        el("span", { class: "badge" }, ["Bästa: ", bestStrong]),
      ]);

      const msg = el("div", { class: "note", text: "Klicka START för att börja." });

      const areaText = el("div", { text: "Starta först" });
      const area = el("div", { class: "reactionArea reactionArea--wait", role: "button", tabindex: "0" }, [areaText]);

      const setArea = (cls, text) => {
        area.className = `reactionArea ${cls}`;
        areaText.textContent = text;
      };

      const start = () => {
        cleanup();
        state = "waiting";
        msg.className = "note";
        msg.textContent = "Vänta…";
        setArea("reactionArea--wait", "Vänta…");

        const base = 700;
        const jitter = 1500;
        const harder = (difficulty - 1) * 120;
        const delay = Math.max(350, base + Math.floor(Math.random() * jitter) - harder);

        timer = setTimeout(() => {
          state = "go";
          goAt = performance.now();
          setArea("reactionArea--go", "KLICKA!");
          msg.className = "note";
          msg.textContent = "NU!";
        }, delay);
      };

      const click = () => {
        if (state === "idle") return;

        if (state === "waiting") {
          cleanup();
          state = "done";
          setArea("reactionArea--tooSoon", "För tidigt");
          msg.className = "note note--bad";
          msg.textContent = "Oj! Du klickade för tidigt. Tryck START och försök igen.";
          return;
        }

        if (state === "go") {
          state = "done";
          const ms = Math.round(performance.now() - goAt);
          const { okMs, greatMs } = thresholds();
          const verdict = ms <= greatMs ? "Super!" : ms <= okMs ? "Bra!" : "Du kan snabbare!";

          setArea("reactionArea--wait", "Klar");
          msg.className = ms <= okMs ? "note note--ok" : "note";
          msg.textContent = `${verdict} ${ms} ms.`;

          if (best === 0 || ms < best) {
            best = ms;
            bestStrong.textContent = `${best} ms`;
            saveInt(STORAGE_KEYS.bestReactionMs, best);
          }
          return;
        }
      };

      const btnStart = el("button", { class: "btn btn--primary", onClick: start, text: "START" });
      const btnResetBest = el("button", {
        class: "btn btn--danger",
        onClick: () => {
          saveInt(STORAGE_KEYS.bestReactionMs, 0);
          renderGame("reaction");
        },
        text: "Nolla bästa",
      });

      area.addEventListener("click", click);
      area.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") click(); });

      return el("div", {}, [title, desc, badgeRow, el("div", { class: "row" }, [btnStart, btnResetBest]), area, msg]);
    };

    return { node: view(), cleanup };
  };

  const gameMath = () => {
    let running = false;
    let score = 0;
    let timeLeft = 0;
    let interval = null;
    let current = null;

    let bestScore = loadInt(STORAGE_KEYS.bestMathScore, 0);

    const settings = () => {
      switch (difficulty) {
        case 1: return { seconds: 30, max: 10, ops: ["+"] };
        case 2: return { seconds: 30, max: 20, ops: ["+", "-"] };
        case 3: return { seconds: 25, max: 50, ops: ["+", "-", "*"] };
        case 4: return { seconds: 22, max: 80, ops: ["+", "-", "*"] };
        case 5: return { seconds: 20, max: 120, ops: ["+", "-", "*"] };
        default: return { seconds: 25, max: 50, ops: ["+", "-", "*"] };
      }
    };

    const makeTask = () => {
      const { max, ops } = settings();
      const a = 1 + Math.floor(Math.random() * max);
      const b = 1 + Math.floor(Math.random() * max);
      const op = ops[Math.floor(Math.random() * ops.length)];
      if (op === "+") return { a, b, op, answer: a + b };
      if (op === "-") {
        const hi = Math.max(a, b);
        const lo = Math.min(a, b);
        return { a: hi, b: lo, op, answer: hi - lo };
      }
      return { a, b, op, answer: a * b };
    };

    const stop = () => {
      running = false;
      if (interval) {
        clearInterval(interval);
        interval = null;
      }
    };

    const view = () => {
      const title = el("h2", { class: "h2", text: "Snabb matte" });
      const desc = el("p", { class: "p", text: `Räkna så många tal som möjligt innan tiden tar slut. Svårighet: ${difficultyLabel()}.` });

      const bestStrong = el("strong", { text: bestScore > 0 ? String(bestScore) : "—" });
      const badgeRow = el("div", { class: "row" }, [
        el("span", { class: "badge" }, ["Poäng: ", el("strong", { id: "mathScore", text: "0" })]),
        el("span", { class: "badge" }, ["Tid: ", el("strong", { id: "mathTime", text: "—" })]),
        el("span", { class: "badge" }, ["Bästa: ", bestStrong]),
      ]);

      const taskEl = el("div", { class: "note", id: "mathTask", text: "Tryck START." });
      const input = el("input", { class: "input", type: "number", inputmode: "numeric", placeholder: "Svar" });
      const msg = el("div", { class: "note", id: "mathMsg", text: "" });

      const update = () => {
        const scoreEl = document.getElementById("mathScore");
        const timeEl = document.getElementById("mathTime");
        if (scoreEl) scoreEl.textContent = String(score);
        if (timeEl) timeEl.textContent = running ? `${timeLeft}s` : "—";
      };

      const next = () => {
        current = makeTask();
        taskEl.className = "note";
        taskEl.textContent = `${current.a} ${current.op} ${current.b} = ?`;
        input.value = "";
        input.focus();
      };

      const start = () => {
        stop();
        const { seconds } = settings();
        running = true;
        score = 0;
        timeLeft = seconds;
        msg.className = "note";
        msg.textContent = "";
        update();
        next();

        interval = setInterval(() => {
          timeLeft -= 1;
          update();
          if (timeLeft <= 0) {
            stop();
            input.blur();
            taskEl.className = "note";
            taskEl.textContent = "Tiden är slut!";
            if (score > bestScore) {
              bestScore = score;
              bestStrong.textContent = String(bestScore);
              saveInt(STORAGE_KEYS.bestMathScore, bestScore);
            }
            msg.className = "note";
            msg.textContent = `Du fick ${score} poäng.`;
          }
        }, 1000);
      };

      const submit = () => {
        if (!running || !current) return;
        const n = Number.parseInt(input.value, 10);
        if (!Number.isFinite(n)) return;

        if (n === current.answer) {
          score += 1;
          msg.className = "note note--ok";
          msg.textContent = "Rätt!";
          update();
          next();
        } else {
          msg.className = "note note--bad";
          msg.textContent = `Fel. Rätt svar var ${current.answer}.`;
          next();
        }
      };

      const btnStart = el("button", { class: "btn btn--primary", onClick: start, text: "START" });
      const btnAnswer = el("button", { class: "btn", onClick: submit, text: "Svara" });
      const btnResetBest = el("button", {
        class: "btn btn--danger",
        onClick: () => {
          saveInt(STORAGE_KEYS.bestMathScore, 0);
          renderGame("math");
        },
        text: "Nolla bästa",
      });

      input.addEventListener("keydown", (e) => { if (e.key === "Enter") submit(); });
      return el("div", {}, [title, desc, badgeRow, el("div", { class: "row" }, [btnStart, input, btnAnswer, btnResetBest]), taskEl, msg]);
    };

    return { node: view(), cleanup: stop };
  };

  const gameSnake = () => {
    const best = loadInt(STORAGE_KEYS.bestSnakeLen, 0);
    const title = el("h2", { class: "h2", text: "Snake" });
    const desc = el("p", { class: "p", text: `Ät maten, väx och undvik väggar/svansen. Styr med piltangenter eller WASD. Svårighet: ${difficultyLabel()}.` });
    const bestStrong = el("strong", { text: best > 0 ? String(best) : "—" });
    const scoreStrong = el("strong", { text: "3" });
    const badgeRow = el("div", { class: "row" }, [
      el("span", { class: "badge" }, ["Längd: ", scoreStrong]),
      el("span", { class: "badge" }, ["Bästa: ", bestStrong]),
    ]);

    const canvas = makeCanvas(560, 420);
    const ctx = canvas.getContext("2d");

    const msg = el("div", { class: "note", text: "Tryck START." });
    const btnStart = el("button", { class: "btn btn--primary", text: "START" });
    const btnRestart = el("button", { class: "btn", text: "Starta om" });

    const wrap = el("div", {}, [title, desc, badgeRow, el("div", { class: "row" }, [btnStart, btnRestart]), canvas, msg]);

    let running = false;
    let interval = null;
    const gridW = 28;
    const gridH = 21;
    const cell = 20;

    let snake = [];
    let dir = { x: 1, y: 0 };
    let nextDir = { x: 1, y: 0 };
    let food = { x: 10, y: 10 };

    const speed = () => {
      switch (difficulty) {
        case 1: return 6;
        case 2: return 8;
        case 3: return 10;
        case 4: return 12;
        case 5: return 14;
        default: return 10;
      }
    };

    const randCell = () => ({ x: Math.floor(Math.random() * gridW), y: Math.floor(Math.random() * gridH) });

    const placeFood = () => {
      for (let i = 0; i < 500; i++) {
        const p = randCell();
        if (!snake.some((s) => s.x === p.x && s.y === p.y)) {
          food = p;
          return;
        }
      }
      food = randCell();
    };

    const reset = () => {
      snake = [{ x: 8, y: 10 }, { x: 7, y: 10 }, { x: 6, y: 10 }];
      dir = { x: 1, y: 0 };
      nextDir = { x: 1, y: 0 };
      placeFood();
      scoreStrong.textContent = String(snake.length);
      msg.className = "note";
      msg.textContent = "Kör!";
      draw();
    };

    const stop = () => {
      running = false;
      if (interval) {
        clearInterval(interval);
        interval = null;
      }
    };

    const gameOver = (text) => {
      stop();
      msg.className = "note note--bad";
      msg.textContent = text;
    };

    const step = () => {
      dir = nextDir;
      const head = snake[0];
      const nh = { x: head.x + dir.x, y: head.y + dir.y };

      if (nh.x < 0 || nh.x >= gridW || nh.y < 0 || nh.y >= gridH) {
        gameOver("Game over: du kraschade i väggen.");
        return;
      }

      const hitSelf = snake.some((s) => s.x === nh.x && s.y === nh.y);
      if (hitSelf) {
        gameOver("Game over: du kraschade i dig själv.");
        return;
      }

      snake.unshift(nh);

      if (nh.x === food.x && nh.y === food.y) {
        placeFood();
      } else {
        snake.pop();
      }

      scoreStrong.textContent = String(snake.length);
      const bestNow = loadInt(STORAGE_KEYS.bestSnakeLen, 0);
      if (snake.length > bestNow) {
        saveInt(STORAGE_KEYS.bestSnakeLen, snake.length);
        bestStrong.textContent = String(snake.length);
      }

      draw();
    };

    const draw = () => {
      if (!ctx) return;
      ctx.clearRect(0, 0, canvas.width, canvas.height);

      ctx.fillStyle = "rgba(15, 23, 41, 0.7)";
      ctx.fillRect(0, 0, canvas.width, canvas.height);

      // food
      ctx.fillStyle = "rgba(255, 107, 107, 0.95)";
      ctx.beginPath();
      ctx.arc(food.x * cell + cell / 2, food.y * cell + cell / 2, cell * 0.38, 0, Math.PI * 2);
      ctx.fill();

      // snake
      for (let i = 0; i < snake.length; i++) {
        const s = snake[i];
        ctx.fillStyle = i === 0 ? "rgba(124, 192, 255, 0.95)" : "rgba(47, 227, 154, 0.85)";
        ctx.fillRect(s.x * cell + 2, s.y * cell + 2, cell - 4, cell - 4);
      }
    };

    const onKey = (e) => {
      const k = e.key;
      const go = (x, y) => {
        // prevent reverse
        if (dir.x === -x && dir.y === -y) return;
        nextDir = { x, y };
      };

      if (k === "ArrowUp" || k === "w" || k === "W") go(0, -1);
      else if (k === "ArrowDown" || k === "s" || k === "S") go(0, 1);
      else if (k === "ArrowLeft" || k === "a" || k === "A") go(-1, 0);
      else if (k === "ArrowRight" || k === "d" || k === "D") go(1, 0);
      else return;

      e.preventDefault();
    };

    const start = () => {
      stop();
      reset();
      running = true;
      interval = setInterval(step, Math.round(1000 / speed()));
    };

    btnStart.addEventListener("click", start);
    btnRestart.addEventListener("click", start);
    window.addEventListener("keydown", onKey);

    reset();
    return { node: wrap, cleanup: () => { stop(); window.removeEventListener("keydown", onKey); } };
  };

  const gameChess = () => {
    const title = el("h2", { class: "h2", text: "Schack" });
    const desc = el("p", { class: "p", text: `Två spelare på samma dator. Grundläggande dragregler (ingen schack/matt-kontroll, ingen rockad/en passant). Svårighet: ${difficultyLabel()}.` });

    const canvas = makeCanvas(560, 560);
    const ctx = canvas.getContext("2d");
    const msg = el("div", { class: "note", text: "Välj en pjäs och klicka på en ruta för att flytta." });
    const turnStrong = el("strong", { text: "Vit" });
    const badgeRow = el("div", { class: "row" }, [el("span", { class: "badge" }, ["Tur: ", turnStrong])]);
    const btnReset = el("button", { class: "btn btn--primary", text: "Ny match" });

    const wrap = el("div", {}, [title, desc, badgeRow, el("div", { class: "row" }, [btnReset]), canvas, msg]);

    const pieces = {
      wp: "♙", wr: "♖", wn: "♘", wb: "♗", wq: "♕", wk: "♔",
      bp: "♟", br: "♜", bn: "♞", bb: "♝", bq: "♛", bk: "♚",
    };

    const inBounds = (r, c) => r >= 0 && r < 8 && c >= 0 && c < 8;
    const colorOf = (p) => (p ? p[0] : null);

    let board = [];
    let turn = "w";
    let selected = null; // {r,c}
    let legal = []; // [{r,c}]

    const reset = () => {
      board = [
        ["br", "bn", "bb", "bq", "bk", "bb", "bn", "br"],
        ["bp", "bp", "bp", "bp", "bp", "bp", "bp", "bp"],
        [null, null, null, null, null, null, null, null],
        [null, null, null, null, null, null, null, null],
        [null, null, null, null, null, null, null, null],
        [null, null, null, null, null, null, null, null],
        ["wp", "wp", "wp", "wp", "wp", "wp", "wp", "wp"],
        ["wr", "wn", "wb", "wq", "wk", "wb", "wn", "wr"],
      ];
      turn = "w";
      turnStrong.textContent = "Vit";
      selected = null;
      legal = [];
      msg.className = "note";
      msg.textContent = "Välj en pjäs och klicka på en ruta för att flytta.";
      draw();
    };

    const rayMoves = (r, c, dr, dc) => {
      const res = [];
      const me = colorOf(board[r][c]);
      let rr = r + dr;
      let cc = c + dc;
      while (inBounds(rr, cc)) {
        const p = board[rr][cc];
        if (!p) res.push({ r: rr, c: cc });
        else {
          if (colorOf(p) !== me) res.push({ r: rr, c: cc });
          break;
        }
        rr += dr;
        cc += dc;
      }
      return res;
    };

    const genMoves = (r, c) => {
      const p = board[r][c];
      if (!p) return [];
      const me = colorOf(p);
      if (me !== turn) return [];
      const t = p[1];
      const res = [];

      const pushIf = (rr, cc) => {
        if (!inBounds(rr, cc)) return;
        const q = board[rr][cc];
        if (!q || colorOf(q) !== me) res.push({ r: rr, c: cc });
      };

      if (t === "p") {
        const dir = me === "w" ? -1 : 1;
        const startRow = me === "w" ? 6 : 1;
        // forward
        if (inBounds(r + dir, c) && !board[r + dir][c]) {
          res.push({ r: r + dir, c });
          if (r === startRow && !board[r + 2 * dir][c]) res.push({ r: r + 2 * dir, c });
        }
        // captures
        for (const dc of [-1, 1]) {
          const rr = r + dir;
          const cc = c + dc;
          if (!inBounds(rr, cc)) continue;
          const q = board[rr][cc];
          if (q && colorOf(q) !== me) res.push({ r: rr, c: cc });
        }
      } else if (t === "r") {
        res.push(...rayMoves(r, c, 1, 0), ...rayMoves(r, c, -1, 0), ...rayMoves(r, c, 0, 1), ...rayMoves(r, c, 0, -1));
      } else if (t === "b") {
        res.push(...rayMoves(r, c, 1, 1), ...rayMoves(r, c, 1, -1), ...rayMoves(r, c, -1, 1), ...rayMoves(r, c, -1, -1));
      } else if (t === "q") {
        res.push(
          ...rayMoves(r, c, 1, 0), ...rayMoves(r, c, -1, 0), ...rayMoves(r, c, 0, 1), ...rayMoves(r, c, 0, -1),
          ...rayMoves(r, c, 1, 1), ...rayMoves(r, c, 1, -1), ...rayMoves(r, c, -1, 1), ...rayMoves(r, c, -1, -1),
        );
      } else if (t === "n") {
        const jumps = [[-2, -1], [-2, 1], [-1, -2], [-1, 2], [1, -2], [1, 2], [2, -1], [2, 1]];
        for (const [dr, dc] of jumps) pushIf(r + dr, c + dc);
      } else if (t === "k") {
        for (const dr of [-1, 0, 1]) for (const dc of [-1, 0, 1]) {
          if (dr === 0 && dc === 0) continue;
          pushIf(r + dr, c + dc);
        }
      }

      return res;
    };

    const move = (fr, fc, tr, tc) => {
      const p = board[fr][fc];
      board[tr][tc] = p;
      board[fr][fc] = null;

      // auto-promote to queen
      if (p && p[1] === "p") {
        if (p[0] === "w" && tr === 0) board[tr][tc] = "wq";
        if (p[0] === "b" && tr === 7) board[tr][tc] = "bq";
      }

      turn = turn === "w" ? "b" : "w";
      turnStrong.textContent = turn === "w" ? "Vit" : "Svart";
      selected = null;
      legal = [];
      draw();
    };

    const draw = () => {
      if (!ctx) return;
      const s = canvas.width;
      const cell = s / 8;

      ctx.clearRect(0, 0, s, s);

      for (let r = 0; r < 8; r++) {
        for (let c = 0; c < 8; c++) {
          const dark = (r + c) % 2 === 1;
          ctx.fillStyle = dark ? "rgba(15, 23, 41, 0.75)" : "rgba(230, 237, 247, 0.08)";
          ctx.fillRect(c * cell, r * cell, cell, cell);

          if (selected && selected.r === r && selected.c === c) {
            ctx.fillStyle = "rgba(124, 192, 255, 0.25)";
            ctx.fillRect(c * cell, r * cell, cell, cell);
          }

          if (legal.some((m) => m.r === r && m.c === c)) {
            ctx.fillStyle = "rgba(47, 227, 154, 0.18)";
            ctx.fillRect(c * cell, r * cell, cell, cell);
          }

          const p = board[r][c];
          if (p) {
            ctx.font = `${Math.floor(cell * 0.72)}px system-ui, Segoe UI Symbol, Arial`;
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            ctx.fillStyle = p[0] === "w" ? "rgba(230, 237, 247, 0.95)" : "rgba(159, 176, 204, 0.95)";
            ctx.fillText(pieces[p], c * cell + cell / 2, r * cell + cell / 2 + 2);
          }
        }
      }
    };

    const onClick = (e) => {
      const rect = canvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;
      const cell = rect.width / 8;
      const c = Math.floor(x / cell);
      const r = Math.floor(y / cell);
      if (!inBounds(r, c)) return;

      if (selected) {
        const ok = legal.some((m) => m.r === r && m.c === c);
        if (ok) {
          move(selected.r, selected.c, r, c);
          return;
        }
      }

      const p = board[r][c];
      if (p && colorOf(p) === turn) {
        selected = { r, c };
        legal = genMoves(r, c);
      } else {
        selected = null;
        legal = [];
      }
      draw();
    };

    btnReset.addEventListener("click", reset);
    canvas.addEventListener("click", onClick);

    reset();
    return { node: wrap, cleanup: () => canvas.removeEventListener("click", onClick) };
  };

  const gameRace = () => {
    let best = loadInt(STORAGE_KEYS.bestRaceScore, 0);
    const title = el("h2", { class: "h2", text: "Bilbana" });
    const desc = el("p", { class: "p", text: `Byt fil med piltangenter eller A/D. Undvik hinder. Svårighet: ${difficultyLabel()}.` });

    const bestStrong = el("strong", { text: best > 0 ? String(best) : "—" });
    const scoreStrong = el("strong", { text: "0" });
    const badgeRow = el("div", { class: "row" }, [
      el("span", { class: "badge" }, ["Poäng: ", scoreStrong]),
      el("span", { class: "badge" }, ["Bästa: ", bestStrong]),
    ]);

    const canvas = makeCanvas(560, 420);
    const ctx = canvas.getContext("2d");
    const msg = el("div", { class: "note", text: "Tryck START." });
    const btnStart = el("button", { class: "btn btn--primary", text: "START" });
    const btnRestart = el("button", { class: "btn", text: "Starta om" });
    const wrap = el("div", {}, [title, desc, badgeRow, el("div", { class: "row" }, [btnStart, btnRestart]), canvas, msg]);

    const lanes = 3;
    let lane = 1;
    let car = { x: 0, y: 0, w: 44, h: 70 };
    let obstacles = [];
    let running = false;
    let raf = 0;
    let last = 0;
    let spawnAcc = 0;
    let score = 0;

    const cfg = () => {
      switch (difficulty) {
        case 1: return { speed: 170, spawnMs: 1000 };
        case 2: return { speed: 210, spawnMs: 900 };
        case 3: return { speed: 250, spawnMs: 800 };
        case 4: return { speed: 295, spawnMs: 700 };
        case 5: return { speed: 340, spawnMs: 620 };
        default: return { speed: 250, spawnMs: 800 };
      }
    };

    const stop = () => {
      running = false;
      if (raf) cancelAnimationFrame(raf);
      raf = 0;
    };

    const reset = () => {
      obstacles = [];
      score = 0;
      scoreStrong.textContent = "0";
      lane = 1;
      const laneW = canvas.width / lanes;
      car.x = laneW * lane + laneW / 2 - car.w / 2;
      car.y = canvas.height - car.h - 14;
      spawnAcc = 0;
      msg.className = "note";
      msg.textContent = "Kör!";
      draw(0);
    };

    const gameOver = () => {
      stop();
      msg.className = "note note--bad";
      msg.textContent = "Krasch! Tryck Starta om.";
    };

    const spawn = () => {
      const laneW = canvas.width / lanes;
      const l = Math.floor(Math.random() * lanes);
      const w = 44;
      const h = 70;
      obstacles.push({
        x: laneW * l + laneW / 2 - w / 2,
        y: -h - 10,
        w,
        h,
      });
    };

    const intersects = (a, b) => !(a.x + a.w < b.x || a.x > b.x + b.w || a.y + a.h < b.y || a.y > b.y + b.h);

    const draw = (t) => {
      if (!ctx) return;
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = "rgba(15, 23, 41, 0.75)";
      ctx.fillRect(0, 0, canvas.width, canvas.height);

      // road lines
      const laneW = canvas.width / lanes;
      ctx.strokeStyle = "rgba(230, 237, 247, 0.12)";
      ctx.lineWidth = 2;
      for (let i = 1; i < lanes; i++) {
        ctx.beginPath();
        ctx.moveTo(laneW * i, 0);
        ctx.lineTo(laneW * i, canvas.height);
        ctx.stroke();
      }

      const dashY = (t / 8) % 30;
      ctx.strokeStyle = "rgba(124, 192, 255, 0.25)";
      for (let i = 0; i < lanes; i++) {
        const cx = laneW * i + laneW / 2;
        ctx.beginPath();
        for (let y = -30; y < canvas.height + 30; y += 30) {
          ctx.moveTo(cx, y + dashY);
          ctx.lineTo(cx, y + dashY + 12);
        }
        ctx.stroke();
      }

      // obstacles
      ctx.fillStyle = "rgba(255, 107, 107, 0.85)";
      for (const o of obstacles) ctx.fillRect(o.x, o.y, o.w, o.h);

      // car
      ctx.fillStyle = "rgba(47, 227, 154, 0.85)";
      ctx.fillRect(car.x, car.y, car.w, car.h);
      ctx.fillStyle = "rgba(11, 18, 32, 0.8)";
      ctx.fillRect(car.x + 10, car.y + 12, car.w - 20, 18);
    };

    const loop = (t) => {
      if (!running) return;
      if (!last) last = t;
      const dt = Math.min(0.05, (t - last) / 1000);
      last = t;

      const { speed, spawnMs } = cfg();
      spawnAcc += dt * 1000;
      if (spawnAcc >= spawnMs) {
        spawnAcc = 0;
        spawn();
      }

      for (const o of obstacles) o.y += speed * dt;
      obstacles = obstacles.filter((o) => o.y < canvas.height + 100);

      for (const o of obstacles) {
        if (intersects(car, o)) {
          gameOver();
          return;
        }
      }

      score += Math.floor(60 * dt);
      scoreStrong.textContent = String(score);
      if (score > best) {
        best = score;
        bestStrong.textContent = String(best);
        saveInt(STORAGE_KEYS.bestRaceScore, best);
      }

      draw(t);
      raf = requestAnimationFrame(loop);
    };

    const setLane = (l) => {
      lane = Math.max(0, Math.min(lanes - 1, l));
      const laneW = canvas.width / lanes;
      car.x = laneW * lane + laneW / 2 - car.w / 2;
    };

    const onKey = (e) => {
      if (e.key === "ArrowLeft" || e.key === "a" || e.key === "A") { setLane(lane - 1); e.preventDefault(); }
      else if (e.key === "ArrowRight" || e.key === "d" || e.key === "D") { setLane(lane + 1); e.preventDefault(); }
    };

    const start = () => {
      stop();
      reset();
      running = true;
      last = 0;
      raf = requestAnimationFrame(loop);
    };

    btnStart.addEventListener("click", start);
    btnRestart.addEventListener("click", start);
    window.addEventListener("keydown", onKey);

    reset();
    return { node: wrap, cleanup: () => { stop(); window.removeEventListener("keydown", onKey); } };
  };

  const gameInvaders = () => {
    let best = loadInt(STORAGE_KEYS.bestInvadersScore, 0);
    const title = el("h2", { class: "h2", text: "Space Invaders" });
    const desc = el("p", { class: "p", text: `Styr med pilar/A/D och skjut med mellanslag. Svårighet: ${difficultyLabel()}.` });
    const bestStrong = el("strong", { text: best > 0 ? String(best) : "—" });
    const scoreStrong = el("strong", { text: "0" });
    const badgeRow = el("div", { class: "row" }, [
      el("span", { class: "badge" }, ["Poäng: ", scoreStrong]),
      el("span", { class: "badge" }, ["Bästa: ", bestStrong]),
    ]);

    const canvas = makeCanvas(560, 420);
    const ctx = canvas.getContext("2d");
    const msg = el("div", { class: "note", text: "Tryck START." });
    const btnStart = el("button", { class: "btn btn--primary", text: "START" });
    const btnRestart = el("button", { class: "btn", text: "Starta om" });
    const wrap = el("div", {}, [title, desc, badgeRow, el("div", { class: "row" }, [btnStart, btnRestart]), canvas, msg]);

    let running = false;
    let raf = 0;
    let last = 0;

    let player = { x: 280, y: 380, w: 44, h: 18, vx: 0 };
    let bullets = [];
    let invBullets = [];
    let inv = [];
    let invDir = 1;
    let invX = 0;
    let invY = 0;
    let invStepAcc = 0;
    let shotCooldown = 0;
    let score = 0;

    const cfg = () => {
      switch (difficulty) {
        case 1: return { invStepMs: 520, invSpeed: 12, fireChance: 0.0012 };
        case 2: return { invStepMs: 460, invSpeed: 14, fireChance: 0.0018 };
        case 3: return { invStepMs: 400, invSpeed: 16, fireChance: 0.0024 };
        case 4: return { invStepMs: 340, invSpeed: 18, fireChance: 0.0030 };
        case 5: return { invStepMs: 290, invSpeed: 20, fireChance: 0.0038 };
        default: return { invStepMs: 400, invSpeed: 16, fireChance: 0.0024 };
      }
    };

    const stop = () => {
      running = false;
      if (raf) cancelAnimationFrame(raf);
      raf = 0;
    };

    const reset = () => {
      score = 0;
      scoreStrong.textContent = "0";
      msg.className = "note";
      msg.textContent = "Kör!";

      player = { x: canvas.width / 2 - 22, y: canvas.height - 40, w: 44, h: 18, vx: 0 };
      bullets = [];
      invBullets = [];
      inv = [];
      invDir = 1;
      invX = 60;
      invY = 50;
      invStepAcc = 0;
      shotCooldown = 0;

      const cols = 10;
      const rows = 5;
      for (let r = 0; r < rows; r++) {
        for (let c = 0; c < cols; c++) {
          inv.push({ x: c * 42, y: r * 34, w: 26, h: 18, alive: true });
        }
      }
      draw(0);
    };

    const gameOver = (text) => {
      stop();
      msg.className = "note note--bad";
      msg.textContent = text;
    };

    const intersects = (a, b) => !(a.x + a.w < b.x || a.x > b.x + b.w || a.y + a.h < b.y || a.y > b.y + b.h);

    const fire = () => {
      if (shotCooldown > 0) return;
      bullets.push({ x: player.x + player.w / 2 - 2, y: player.y - 10, w: 4, h: 10, vy: -420 });
      shotCooldown = 0.22;
    };

    const draw = (t) => {
      if (!ctx) return;
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      ctx.fillStyle = "rgba(15, 23, 41, 0.75)";
      ctx.fillRect(0, 0, canvas.width, canvas.height);

      // stars
      ctx.fillStyle = "rgba(230, 237, 247, 0.12)";
      for (let i = 0; i < 60; i++) {
        const x = (i * 97 + (t / 10)) % canvas.width;
        const y = (i * 53 + (t / 22)) % canvas.height;
        ctx.fillRect(x, y, 2, 2);
      }

      // invaders
      ctx.fillStyle = "rgba(124, 192, 255, 0.85)";
      for (const it of inv) {
        if (!it.alive) continue;
        ctx.fillRect(invX + it.x, invY + it.y, it.w, it.h);
      }

      // bullets
      ctx.fillStyle = "rgba(47, 227, 154, 0.9)";
      for (const b of bullets) ctx.fillRect(b.x, b.y, b.w, b.h);
      ctx.fillStyle = "rgba(255, 107, 107, 0.85)";
      for (const b of invBullets) ctx.fillRect(b.x, b.y, b.w, b.h);

      // player
      ctx.fillStyle = "rgba(47, 227, 154, 0.85)";
      ctx.fillRect(player.x, player.y, player.w, player.h);
      ctx.fillRect(player.x + player.w / 2 - 4, player.y - 10, 8, 10);
    };

    const loop = (t) => {
      if (!running) return;
      if (!last) last = t;
      const dt = Math.min(0.05, (t - last) / 1000);
      last = t;

      shotCooldown = Math.max(0, shotCooldown - dt);

      player.x += player.vx * dt;
      player.x = Math.max(0, Math.min(canvas.width - player.w, player.x));

      for (const b of bullets) b.y += b.vy * dt;
      bullets = bullets.filter((b) => b.y + b.h >= -20);

      for (const b of invBullets) b.y += b.vy * dt;
      invBullets = invBullets.filter((b) => b.y <= canvas.height + 30);

      // invader stepping
      const { invStepMs, invSpeed, fireChance } = cfg();
      invStepAcc += dt * 1000;
      if (invStepAcc >= invStepMs) {
        invStepAcc = 0;
        invX += invDir * invSpeed;

        // compute bounds
        let minX = Infinity;
        let maxX = -Infinity;
        let maxY = -Infinity;
        for (const it of inv) {
          if (!it.alive) continue;
          minX = Math.min(minX, invX + it.x);
          maxX = Math.max(maxX, invX + it.x + it.w);
          maxY = Math.max(maxY, invY + it.y + it.h);
        }

        if (minX < 10 || maxX > canvas.width - 10) {
          invDir *= -1;
          invY += 18;
        }

        if (maxY > player.y - 6) {
          gameOver("Game over: invaders nådde dig.");
          return;
        }
      }

      // invader shooting
      if (Math.random() < fireChance) {
        // pick random alive invader
        const alive = inv.filter((i) => i.alive);
        if (alive.length) {
          const it = alive[Math.floor(Math.random() * alive.length)];
          invBullets.push({ x: invX + it.x + it.w / 2 - 2, y: invY + it.y + it.h, w: 4, h: 10, vy: 260 });
        }
      }

      // bullet collisions
      for (const b of bullets) {
        for (const it of inv) {
          if (!it.alive) continue;
          const box = { x: invX + it.x, y: invY + it.y, w: it.w, h: it.h };
          if (intersects(b, box)) {
            it.alive = false;
            b.y = -9999;
            score += 10;
          }
        }
      }
      bullets = bullets.filter((b) => b.y > -1000);

      for (const b of invBullets) {
        if (intersects(b, player)) {
          gameOver("Game over: du blev träffad.");
          return;
        }
      }

      const aliveCount = inv.reduce((acc, it) => acc + (it.alive ? 1 : 0), 0);
      if (aliveCount === 0) {
        msg.className = "note note--ok";
        msg.textContent = "Du vann! Tryck Starta om för en ny runda.";
        stop();
        return;
      }

      scoreStrong.textContent = String(score);
      if (score > best) {
        best = score;
        bestStrong.textContent = String(best);
        saveInt(STORAGE_KEYS.bestInvadersScore, best);
      }

      draw(t);
      raf = requestAnimationFrame(loop);
    };

    const onKeyDown = (e) => {
      if (e.key === "ArrowLeft" || e.key === "a" || e.key === "A") { player.vx = -280; e.preventDefault(); }
      else if (e.key === "ArrowRight" || e.key === "d" || e.key === "D") { player.vx = 280; e.preventDefault(); }
      else if (e.key === " " || e.key === "Spacebar") { fire(); e.preventDefault(); }
    };
    const onKeyUp = (e) => {
      if (e.key === "ArrowLeft" || e.key === "a" || e.key === "A") { if (player.vx < 0) player.vx = 0; }
      else if (e.key === "ArrowRight" || e.key === "d" || e.key === "D") { if (player.vx > 0) player.vx = 0; }
    };

    const start = () => {
      stop();
      reset();
      running = true;
      last = 0;
      raf = requestAnimationFrame(loop);
    };

    btnStart.addEventListener("click", start);
    btnRestart.addEventListener("click", start);
    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("keyup", onKeyUp);

    reset();
    return { node: wrap, cleanup: () => { stop(); window.removeEventListener("keydown", onKeyDown); window.removeEventListener("keyup", onKeyUp); } };
  };

  const games = {
    guess: gameGuess,
    reaction: gameReaction,
    math: gameMath,
    snake: gameSnake,
    chess: gameChess,
    race: gameRace,
    invaders: gameInvaders,
  };

  const renderGame = (gameId) => {
    if (typeof currentCleanup === "function") {
      try { currentCleanup(); } catch { /* ignore */ }
    }
    currentCleanup = null;
    clearRoot();
    setNav(gameId);

    const gameFn = games[gameId] || games.guess;
    const out = gameFn();
    const node = out && typeof out === "object" && out.node ? out.node : out;
    currentCleanup = out && typeof out === "object" && typeof out.cleanup === "function" ? out.cleanup : null;
    gameRoot.appendChild(node);
  };

  document.querySelectorAll(".nav__btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = btn.dataset.game;
      if (!id) return;
      location.hash = `#${id}`;
      renderGame(id);
    });
  });

  const initial = (() => {
    const hash = (location.hash || "").replace(/^#/, "").trim();
    if (hash && games[hash]) return hash;
    return "guess";
  })();

  renderGame(initial);
})();
