/**
 * @author: Matthias Christinan Jasny <mjasny>
 * @email:  matthiaschristian.jasny@stud.tu-darmstadt.de
 * @project: DB4ML
 * @filename: mpmc_queue.hpp
 * @last modified by: mjasny

 * @description: Multithreaded lockfree queue, which supports any arbitrary data
 * type and has custom allocator support. Faster than boost::lockfree_queue
 * Adapted from:
 * https://github.com/jk983294/concurrent/blob/master/src/lockfree/MpmcBoundedQueue.h
 */



#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <thread>
#include <stdexcept>


namespace dfi {

template <typename T, class Allocator=std::allocator<T>> class MPMCQueue {
    typedef typename Allocator::template rebind<MPMCQueue>::other This_alloc_type;
    typedef typename Allocator::template rebind<T>::other T_alloc_type;

private:
    static_assert(std::is_nothrow_copy_assignable<T>::value ||
                    std::is_nothrow_move_assignable<T>::value,
                "T must be nothrow copy or move assignable");

    static_assert(std::is_nothrow_destructible<T>::value,
                "T must be nothrow destructible");



  T_alloc_type allocator;

public:
    explicit MPMCQueue(const size_t capacity)
    : capacity_(capacity), head_(0), tail_(0) {
        initialize();
    }

    explicit MPMCQueue(const size_t capacity, T_alloc_type const& allocator)
    : allocator(allocator), capacity_(capacity), head_(0), tail_(0) {
        // std::cout << "copied?" << std::endl;
        initialize();
    }

    ~MPMCQueue() noexcept {
        for (size_t i = 0; i < capacity_; ++i) {
            slots_[i].~Slot();
        }
        // free(buf_);
        size_t space = capacity_ * sizeof(Slot) + kCacheLineSize - 1;
        allocator.deallocate((T*) buf_, space);
    }

    static void *operator new(std::size_t sz, This_alloc_type allocator) {
        // std::cout << "custom new for size " << sz << '\n';
        return allocator.allocate(1);
    }

    static void operator delete(void* ptr, std::size_t sz, This_alloc_type allocator) {
        // std::cout << "custom delete for size " << sz << std::endl;
        return allocator.deallocate(static_cast<MPMCQueue*>(ptr), sz);          // not numa node specific
    }

    void* get_buffer() {
        return buf_;
    }

  // non-copyable and non-movable
    MPMCQueue(const MPMCQueue &) = delete;
    MPMCQueue &operator=(const MPMCQueue &) = delete;

    template <typename... Args> void emplace(Args &&... args) noexcept {
        static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
          "T must be nothrow constructible with Args&&...");
        auto const head = head_.fetch_add(1);
        auto &slot = slots_[idx(head)];
        while (turn(head) * 2 != slot.turn.load(std::memory_order_acquire))
            ;
        slot.construct(std::forward<Args>(args)...);
        slot.turn.store(turn(head) * 2 + 1, std::memory_order_release);
    }

    template <typename... Args> bool try_emplace(Args &&... args) noexcept {
        static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
          "T must be nothrow constructible with Args&&...");
        auto head = head_.load(std::memory_order_acquire);
        for (;;) {
          auto &slot = slots_[idx(head)];
          if (turn(head) * 2 == slot.turn.load(std::memory_order_acquire)) {
              if (head_.compare_exchange_strong(head, head + 1)) {
                  slot.construct(std::forward<Args>(args)...);
                  slot.turn.store(turn(head) * 2 + 1, std::memory_order_release);
                  return true;
              }
          } else {
              auto const prevHead = head;
              head = head_.load(std::memory_order_acquire);
              if (head == prevHead) {
                  return false;
              }
          }
        }
    }

    void push(const T &v) noexcept {
        static_assert(std::is_nothrow_copy_constructible<T>::value,
          "T must be nothrow copy constructible");
        emplace(v);
    }

    template <typename P,
            typename = typename std::enable_if<
                std::is_nothrow_constructible<T, P &&>::value>::type>
    void push(P &&v) noexcept {
        emplace(std::forward<P>(v));
    }

    bool try_push(const T &v) noexcept {
        static_assert(std::is_nothrow_copy_constructible<T>::value,
            "T must be nothrow copy constructible");
        return try_emplace(v);
    }

    template <typename P,
            typename = typename std::enable_if<
                std::is_nothrow_constructible<T, P &&>::value>::type>
    bool try_push(P &&v) noexcept {
        return try_emplace(std::forward<P>(v));
    }

    void pop(T &v) noexcept {
        auto const tail = tail_.fetch_add(1);
        auto &slot = slots_[idx(tail)];
        while (turn(tail) * 2 + 1 != slot.turn.load(std::memory_order_acquire))
            ;
        v = slot.move();
        slot.destroy();
        slot.turn.store(turn(tail) * 2 + 2, std::memory_order_release);
    }

    bool try_pop(T &v) noexcept {
        auto tail = tail_.load(std::memory_order_acquire);
        for (;;) {
            auto &slot = slots_[idx(tail)];
            if (turn(tail) * 2 + 1 == slot.turn.load(std::memory_order_acquire)) {
                if (tail_.compare_exchange_strong(tail, tail + 1)) {
                    v = slot.move();
                    slot.destroy();
                    slot.turn.store(turn(tail) * 2 + 2, std::memory_order_release);
                    return true;
                }
            } else {
                auto const prevTail = tail;
                tail = tail_.load(std::memory_order_acquire);
                if (tail == prevTail) {
                    return false;
                }
            }
        }
    }

    size_t size() {
        return tail_.load() - head_.load();
    }


private:
    constexpr size_t idx(size_t i) const noexcept { return i % capacity_; }

    constexpr size_t turn(size_t i) const noexcept { return i / capacity_; }

    static constexpr size_t kCacheLineSize = 128;

    void initialize() {
        // std::cout << "queue initialized" << std::endl;
        if (capacity_ < 1) {
            throw std::invalid_argument("capacity < 1");
        }
        size_t space = capacity_ * sizeof(Slot) + kCacheLineSize - 1;
        // buf_ = malloc(space);
        buf_ = allocator.allocate(space);
        if (buf_ == nullptr) {
            throw std::bad_alloc();
        }
        void *buf = buf_;
        slots_ = reinterpret_cast<Slot *>(
            std::align(kCacheLineSize, capacity_ * sizeof(Slot), buf, space));
            if (slots_ == nullptr) {
                // free(buf_);
                allocator.deallocate((T*) buf_, space);
                throw std::bad_alloc();
            }
            for (size_t i = 0; i < capacity_; ++i) {
                new (&slots_[i]) Slot();
            }
            static_assert(sizeof(MPMCQueue<T>) % kCacheLineSize == 0,
                "MPMCQueue<T> size must be a multiple of cache line size to "
                "prevent false sharing between adjacent queues");
            static_assert(sizeof(Slot) % kCacheLineSize == 0,
                "Slot size must be a multiple of cache line size to prevent "
                "false sharing between adjacent slots");
            assert(reinterpret_cast<size_t>(slots_) % kCacheLineSize == 0 &&
                "slots_ array must be aligned to cache line size to prevent false "
                "sharing between adjacent slots");
            assert(reinterpret_cast<char *>(&tail_) -
                reinterpret_cast<char *>(&head_) >=
                kCacheLineSize &&
                "head and tail must be a cache line apart to prevent false sharing");
        }

        struct Slot {
            ~Slot() noexcept {
                if (turn & 1) {
                    destroy();
                }
            }

            template <typename... Args> void construct(Args &&... args) noexcept {
                static_assert(std::is_nothrow_constructible<T, Args &&...>::value,
                    "T must be nothrow constructible with Args&&...");
                new (&storage) T(std::forward<Args>(args)...);
            }

            void destroy() noexcept {
                static_assert(std::is_nothrow_destructible<T>::value,
                    "T must be nothrow destructible");
                reinterpret_cast<T *>(&storage)->~T();
            }

            T &&move() noexcept { return reinterpret_cast<T &&>(storage); }

            // Align to avoid false sharing between adjacent slots
            alignas(kCacheLineSize) std::atomic<size_t> turn = {0};
            typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
        };

private:
    const size_t capacity_;
    Slot *slots_;
    void *buf_;

    // Align to avoid false sharing between head_ and tail_
    alignas(kCacheLineSize) std::atomic<size_t> head_;
    alignas(kCacheLineSize) std::atomic<size_t> tail_;
};
}