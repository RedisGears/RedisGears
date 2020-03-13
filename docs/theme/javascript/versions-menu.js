import { versions, latest } from '/javascript/versions.mjs'
import { branch } from './branch.mjs'

// Versions submenu for mkdocs-material theme
// Credit: https://github.com/containous/structor
function addMaterialMenu(elt, versions) {
  const current = versions.find(e => e.branch == branch);
  const rootLi = document.createElement('li');
  rootLi.classList.add('md-nav__item');
  rootLi.classList.add('md-nav__item--nested');
  rootLi.classList.add('md-nav__item--version');

  const input = document.createElement('input');
  input.classList.add('md-toggle');
  input.classList.add('md-nav__toggle');
  input.setAttribute('data-md-toggle', 'nav-10000000');
  input.id = "nav-10000000";
  input.type = 'checkbox';

  rootLi.appendChild(input);

  const lbl01 = document.createElement('label')
  lbl01.classList.add('md-nav__link');
  lbl01.setAttribute('for', 'nav-10000000');
  lbl01.textContent = current.text + " " + (
    current.branch == latest
      ? "(latest) "
      : ""
    );

  rootLi.appendChild(lbl01);

  const nav = document.createElement('nav')
  nav.classList.add('md-nav');
  nav.setAttribute('data-md-component','collapsible');
  nav.setAttribute('data-md-level','1');

  rootLi.appendChild(nav);

  const lbl02 = document.createElement('label')
  lbl02.classList.add('md-nav__title');
  lbl02.setAttribute('for', 'nav-10000000');
  lbl02.textContent = current.text + " ";

  nav.appendChild(lbl02);

  const ul = document.createElement('ul')
  ul.classList.add('md-nav__list');
  ul.setAttribute('data-md-scrollfix','');

  nav.appendChild(ul);

  for (let i = 0; i < versions.length; i++) {
    const li = document.createElement('li');
    li.classList.add('md-nav__item');

    ul.appendChild(li);

    const a = document.createElement('a');
    a.classList.add('md-nav__link');
    if (versions[i].branch == current.branch) {
      a.classList.add('md-nav__link--active');
    }

    a.href = window.location.protocol + "//" + window.location.host + "/";
    if (versions[i].path) {
      a.href = a.href + versions[i].path + "/"
    }
    const txt = versions[i].text + " " + (
      versions[i].branch == latest
        ? "(latest) "
        : ""
    );
    a.title = txt;
    a.text = txt;

    li.appendChild(a);
  }

  elt.appendChild(rootLi);
}

const materialSelector = 'div.md-container main.md-main div.md-main__inner.md-grid div.md-sidebar.md-sidebar--primary div.md-sidebar__scrollwrap div.md-sidebar__inner nav.md-nav.md-nav--primary ul.md-nav__list';
const elt = document.querySelector(materialSelector);
addMaterialMenu(elt, versions);