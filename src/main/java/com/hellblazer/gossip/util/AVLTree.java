/** (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package com.hellblazer.gossip.util;

//import AVLNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class AVLTree<E> implements SortedCollection<E> {
    public class MyIterator implements Iterator<E> {
        private ArrayList<E> _list;
        private E            _next;

        public MyIterator(ArrayList<E> els) {
            _next = null;
            _list = els;

        }

        @Override
        public boolean hasNext() {

            return !_list.isEmpty();
        }

        @Override
        public E next() {

            if (hasNext()) {
                _next = _list.remove(0);
                return _next;
            }
            return null;
        }

        @Override
        public void remove() {
            AVLTree.this.remove(_next);

        }
    }

    static enum Traversal {
        PREORDER, INORDER, POSTORDER, BYLEVEL
    }

    /** The root of the Tree. */

    private AVLNode<E>            _root;

    /** Size of the Tree. */

    private int                   _size = 0;

    /** The Traversal data member. */

    private Traversal             _traversal;

    /** ArrayList to hold the Elements. */

    private ArrayList<E>          _myElements;

    /** Comparator used to compare Items in the Tree.. */

    private Comparator<? super E> _comp;

    boolean                       contains;

    /**
     * The First Constructor which constructs a new AVLTree where the nodes
     * should be compared and stored obey's the given comparator.
     * 
     * @param comparator
     *            - comparator which will compare the elements would be added to
     *            the AVLTree
     */
    public AVLTree(Comparator<? super E> comparator) {
        _root = null;
        if (comparator == null) {
            throw new NullPointerException("Invalid Comparator");
        }
        _comp = comparator;

        _traversal = Traversal.INORDER;
    }

    /**
     * @param col
     */
    public AVLTree(SortedCollection<E> col) {
        _traversal = Traversal.INORDER;
        _size = col.size();
        _comp = col.comparator();
        for (E element : col) {
            add(element);
        }

    }

    @Override
    public void add(E element) {
        if (element == null) {
            throw new NullPointerException("Invalid element to be added.");
        }
        insert(element, _root);

    }

    /*
     * (non-Javadoc)
     * 
     * @seeoop.ex4.collection.SortedCollection#addAll(oop.ex4.collection.
     * SortedCollection)
     */
    @Override
    public void addAll(SortedCollection<? extends E> col) {
        if (col == null) {
            throw new NullPointerException(
                                           "Null/Invalid Collection has been passed. ");
        }
        for (E e : col) {
            add(e);
        }
    }

    @Override
    public Comparator<? super E> comparator() {
        return this._comp;
    }

    @Override
    public boolean contains(E element) {
        contains = false;
        contains(new AVLNode<E>(element), _root);

        return contains;
    }

    @Override
    public E find(E element) {
        if (element == null) {
            throw new NullPointerException(
                                           "Null Element Cannot be Stored in Tree's Nodes");
        }
        if (_root == null) {
            throw new NullPointerException("Empty Tree.");
        }
        AVLNode<E> found = findNode(element);
        return found == null ? null : found._data;
    }

    @Override
    public E first() {
        return min(_root)._data;
    }

    // switch (_traversal) {
    // case POSTORDER:
    // postorder(_root);
    // // return new myIterator<E>(Traversal.POSTORDER);
    //
    // case PREORDER:
    // preorder(_root);
    // // return new myIterator<E>(Traversal.PREORDER);
    //
    // case INORDER:
    // inorder(_root);
    // break;
    // case BYLEVEL:
    // bylevel(_root, 0, new ArrayList<ArrayList<E>>());
    // // return new myIterator<E>(Traversal.BYLEVEL);
    //
    // default:
    // inorder(_root);
    // }
    // return new myIterator(_traversal);
    //
    // }

    /**
     * Gets the traversal.
     * 
     * @return The Traversal.
     */
    public Traversal getTravel() {

        return this._traversal;
    }

    public int height(AVLNode<E> t) {
        return t == null ? -1 : t._height;
    }

    @Override
    public boolean isEmpty() {
        return _root == null;
    }

    @Override
    public Iterator<E> iterator() {

        _myElements = new ArrayList<E>();

        switch (_traversal) {

            case INORDER: {
                inorder(_root);
                return new MyIterator(_myElements);
            }
            case PREORDER: {
                preorder(_root);
                return new MyIterator(_myElements);
            }
            case POSTORDER: {
                postorder(_root);
                return new MyIterator(_myElements);
            }
            case BYLEVEL: {
                bylevel(_root, 0, new ArrayList<ArrayList<E>>());
                return new MyIterator(_myElements);
            }
            default: {
                inorder(_root);
                return new MyIterator(_myElements);
            }
        }
    }

    @Override
    public E last() {
        return max(_root)._data;
    }

    public AVLNode<E> remove(AVLNode<E> curr, E data) {

        if (curr != null) {
            if (curr.getData() == data) {
                if (curr.getData() == null || curr._right == null) {
                    AVLNode<E> temp = curr._right == null ? curr._left
                                                         : curr._right;
                    curr = null;
                    return temp;
                }
                AVLNode<E> heir = curr._left;
                while (heir._right != null) {
                    heir = heir._right;
                }
                curr._data = heir.getData();
                data = heir.getData();
                if (_comp.compare(data, curr.getData()) > 0) {

                    curr._right = remove(curr._right, data);
                    if (getHeight(curr._right) - getHeight(curr._left) == -3) {
                        if (getHeight(curr._left._left) <= getHeight(curr._left._right)) {
                            curr = rotateWithLeftChild(curr);
                        } else {
                            curr = doubleWithLeftChild(curr);
                        }
                    } else {
                        curr._height = Math.max(getHeight(curr._left),
                                                getHeight(curr._right)) + 1;// 
                    }
                } else if (_comp.compare(data, curr.getData()) <= 0) {
                    curr._left = remove(curr._left, data);
                    if (getHeight(curr._left) - getHeight(curr._left) == -3) {
                        if (getHeight(curr._right._right) >= getHeight(curr._right._left)) {
                            curr = rotateWithRightChild(curr);
                        } else {
                            curr = doubleWithRightChild(curr);
                        }
                    } else {
                        curr._height = Math.max(getHeight(curr._left),
                                                getHeight(curr._right)) + 1;
                    }
                }
            }
        }
        return curr;

    }

    @Override
    public void remove(E element) {
        if (element == null) {
            throw new IllegalArgumentException();
        }
        remove(_root, element);
    }

    /**
     * Sets the given traverser .
     * 
     * @param traverser
     */
    public void setTravel(Traversal traverser) {

        _traversal = traverser;
    }

    @Override
    public int size() {
        return _size;
    }

    @Override
    public Iterable<E> subset(E from, E to) {
        if (_comp.compare(from, to) >= 0) {
            // System.out.println("%%%%%%%%%%%");
            return new AVLTree<E>(_comp);
        }
        AVLTree<E> tree = new AVLTree<E>(_comp);
        Iterator<E> itr = this.iterator();
        while (itr.hasNext()) {
            E e = itr.next();
            if (_comp.compare(from, e) <= 0 && _comp.compare(to, e) > 0) {
                tree.add(e);
            }
        }
        return tree;
    }

    private void bylevel(AVLNode<E> node, int level,
                         ArrayList<ArrayList<E>> listOfArrays) {
        ArrayList<E> levelNodes;
        try {
            levelNodes = listOfArrays.get(level);
        } catch (IndexOutOfBoundsException e) {
            listOfArrays.add(levelNodes = new ArrayList<E>());
        }
        levelNodes.add(node._data);
        level++;
        if (node._left != null) {
            bylevel(node._left, level, listOfArrays);
        }
        if (node._right != null) {
            bylevel(node._right, level, listOfArrays);
        }
        if (node == _root) {
            for (ArrayList<E> nodeList : listOfArrays) {
                for (E element : nodeList) {
                    add(element);
                }
            }
        }
    }

    private void contains(AVLNode<E> lookFor, AVLNode<E> currentNode) {

        if (currentNode == null) {
            return;
        }
        if (_comp.compare(currentNode._data, lookFor._data) == 0)

        {
            contains = true;
            return;
        } else if (_comp.compare(lookFor._data, currentNode._data) < 0)

        {
            contains(lookFor, currentNode._left);
        } else {
            contains(lookFor, currentNode._right);
        }
        return;
    }

    // insert function that insert anode in the tree and check the balance
    // after insertion

    private AVLNode<E> doubleWithLeftChild(AVLNode<E> k3) {
        k3._left = rotateWithRightChild(k3._left);
        return rotateWithLeftChild(k3);
    }

    private AVLNode<E> doubleWithRightChild(AVLNode<E> k1) {
        k1._right = rotateWithLeftChild(k1._right);
        return rotateWithRightChild(k1);
    }

    private AVLNode<E> findNode(E element) {

        AVLNode<E> node = _root;
        AVLNode<E> tmp = null;

        do {
            int comarison = _comp.compare(element, node._data);
            if (comarison == 0) {
                return node;
            }
            if (comarison >= 0) {
                tmp = node;
                node = node._right;
            } else {
                node = node._left;
            }

        } while (node != null);
        return tmp != null ? tmp : null;
    }

    // HEIGHT GETTER.
    private int getHeight(AVLNode<E> node) {
        if (node == null) {
            return -1;
        }
        return node._height;
    }

    // private void inorder(AVLNode<E> node) {
    // if (node._left != null)
    // inorder(node._left);
    // add(node._data);
    // if (node._right != null)
    // inorder(node._right);
    // }
    private void inorder(AVLNode<E> current) {
        if (current != null) {
            inorder(current._left);
            _myElements.add(current._data);
            // System.out.print(current._element+", ");
            inorder(current._right);
        }
    }

    private AVLNode<E> insert(E x, AVLNode<E> t) {
        if (t == null) {
            // this procedure do the insertion
            {
                t = new AVLNode<E>(x);
                t._parent = null;
            }
            if (_size == 0) {
                _root = t;
            }
            _size++;
        }
        // int compareResult = _comp.compare(x, t._data);

        else if (_comp.compare(x, t._data) <= 0) {
            t._left = insert(x, t._left);
            if (getHeight(t._left) - getHeight(t._right) == 3) {
                if (_comp.compare(x, t._left._data) < 0) {
                    t = rotateWithLeftChild(t);
                } else {
                    t = doubleWithLeftChild(t);
                }
            }
        } else if (_comp.compare(x, t._data) > 0) {
            t._right = insert(x, t._right);
            if (getHeight(t._right) - getHeight(t._left) == 3) {
                if (_comp.compare(x, t._right._data) > 0) {
                    t = rotateWithRightChild(t);
                } else {
                    t = doubleWithRightChild(t);
                }
            }
        }

        t._height = Math.max(getHeight(t._left), getHeight(t._right)) + 1;
        return t;
    }

    private AVLNode<E> max(AVLNode<E> node) {
        if (isEmpty() || node == null) {
            return null;
        }
        while (node._right != null) {
            node = node._right;
        }
        return node;
    }

    private AVLNode<E> min(AVLNode<E> node) {
        if (isEmpty() || node == null) {
            return null;
        }
        while (node._left != null) {
            node = node._left;
        }
        return node;
    }

    private void postorder(AVLNode<E> node) {
        if (node != null) {
            postorder(node._left);

            postorder(node._right);
            _myElements.add(node._data);
        }
    }

    private void preorder(AVLNode<E> node) {
        if (node != null) {
            _myElements.add(node._data);

            preorder(node._left);

            preorder(node._right);
        }
    }

    private AVLNode<E> rotateWithLeftChild(AVLNode<E> k2) {
        AVLNode<E> tempParent = k2._parent;
        AVLNode<E> k1 = k2._left; // k2 will be in position of k1
        {
            if (tempParent != null && tempParent._left == k2) {
                tempParent._left = k1;
            } else if (tempParent != null && tempParent._right == k2) {
                tempParent._right = k1;
            }
        }
        k2._left = k1._right;
        if (k1._right != null) {
            k1._right._parent = k2;
        }
        k1._right = k2;
        k1._parent = k2._parent;
        k2._parent = k1;
        // set the height of the rearranged Nodes
        k2._height = Math.max(getHeight(k2._left), getHeight(k2._right)) + 1;
        k1._height = Math.max(getHeight(k1._left), k1._height) + 1;
        if (k2 == _root) {
            _root = k1; // if k1 was the root, k2 will be the route
        }
        return k1;
        // AVLNode<E> k1 = k2._left;
        // k2._left = k1._right;
        // k1._right = k2;
        // k2._height = Math.max(getHeight(k2._left), getHeight(k2._right)) + 1;
        // k1._height = Math.max(getHeight(k1._left), k2._height) + 1;
        // return k1;
    }

    private AVLNode<E> rotateWithRightChild(AVLNode<E> k1) {

        AVLNode<E> tempParent = k1._parent;
        AVLNode<E> k2 = k1._right;
        k2._parent = tempParent;

        {
            if (tempParent != null && tempParent._left == k1) {
                tempParent._left = k2;
            } else if (tempParent != null && tempParent._right == k1) {
                tempParent._right = k2;
            }
        }
        k1._right = k2._left;
        if (k2._left != null) {
            k2._left._parent = k1;
        }

        k2._left = k1;
        k1._parent = k2;
        k1._height = Math.max(getHeight(k1._left), getHeight(k1._right)) + 1;
        k2._height = Math.max(getHeight(k2._left), k1._height) + 1;

        if (k1 == _root) {
            _root = k2;
        }
        return k2;
    }

}
