function switch_tab(evt, tab_id) {
  var i, tabcontent, tablinks, tab;

  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {
    tabcontent[i].style.display = "none";
  }
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }

  tab = document.getElementById(tab_id);

  /* Lazy-load <object> elements: the tab itself may be an <object>,
     or it may be a container with child <object>s */
  if (tab.tagName === "OBJECT") {
    if (tab.dataset.src && !tab.getAttribute("data")) {
      tab.setAttribute("data", tab.dataset.src);
    }
  } else {
    var objects = tab.getElementsByTagName("object");
    for (i = 0; i < objects.length; i++) {
      if (objects[i].dataset.src && !objects[i].getAttribute("data")) {
        objects[i].setAttribute("data", objects[i].dataset.src);
      }
    }
  }

  tab.style.display = "block";
  evt.currentTarget.className += " active";
  evt.target.classList.toggle('transition');
}
