(function(){
  const key = "theme";
  const prefersLight = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches;
  const initial = localStorage.getItem(key) || (prefersLight ? "light" : "dark");
  if(initial === "light") document.body.classList.add("light");

  const btn = document.getElementById("theme-toggle");
  if(!btn) return;
  btn.addEventListener("click", ()=>{
    const isLight = document.body.classList.toggle("light");
    localStorage.setItem(key, isLight ? "light" : "dark");
  });
})();