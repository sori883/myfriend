import * as THREE from 'three'
import { VRM, VRMHumanBoneName } from '@pixiv/three-vrm'
import { EmotionType } from '@/features/messages/messages'
import {
  GESTURE_POSES,
  GESTURE_SLERP_SPEED,
  BREATHING,
  IDLE_SWAY,
} from './motionConstants'

const DEG2RAD = Math.PI / 180

/** 度数からクォータニオンへ変換 */
const eulerDegToQuat = (
  pitch: number,
  yaw: number,
  roll: number
): THREE.Quaternion => {
  return new THREE.Quaternion().setFromEuler(
    new THREE.Euler(pitch * DEG2RAD, yaw * DEG2RAD, roll * DEG2RAD, 'YXZ')
  )
}

/**
 * 感情に応じたボディジェスチャーを制御するクラス
 *
 * idle_loop.vrma のアイドルアニメーション上に、
 * 小さな回転オフセットを加算する。
 * AnimationMixer.update() の後、vrm.update() の前に applyGesture() を呼ぶ。
 *
 * mixer がアニメーションしないボーンでの回転累積を防ぐため、
 * 初回アクセス時にベースクォータニオンを保存し、
 * 毎フレーム「ベース → ジェスチャー → オシレーション」の順で上書きする。
 */
export class MotionController {
  private _currentRotations: Map<VRMHumanBoneName, THREE.Quaternion>
  private _targetRotations: Map<VRMHumanBoneName, THREE.Quaternion>
  private _allUsedBones: ReadonlySet<VRMHumanBoneName>
  private _elapsedTime: number

  /** 初回アクセス時のボーンクォータニオン（累積防止用） */
  private _baseQuats: Map<VRMHumanBoneName, THREE.Quaternion>

  // _applyOscillation 用の再利用オブジェクト（同期呼び出しのため安全）
  private _tempQuat: THREE.Quaternion
  private _tempEuler: THREE.Euler

  constructor() {
    this._currentRotations = new Map()
    this._targetRotations = new Map()
    this._baseQuats = new Map()
    // 全ポーズで使われるボーンを事前計算
    this._allUsedBones = new Set(
      Object.values(GESTURE_POSES).flatMap((pose) =>
        pose.map((p) => p.bone)
      )
    )
    this._elapsedTime = 0
    this._tempQuat = new THREE.Quaternion()
    this._tempEuler = new THREE.Euler()
  }

  /** 目標の感情ポーズを設定（遷移は update() でスムーズに行われる） */
  public setEmotion(emotion: EmotionType): void {
    const pose = GESTURE_POSES[emotion]
    if (!pose) return

    const newTargets = new Map<VRMHumanBoneName, THREE.Quaternion>()
    const poseBones = new Set<VRMHumanBoneName>()

    for (const entry of pose) {
      newTargets.set(
        entry.bone,
        eulerDegToQuat(entry.pitch, entry.yaw, entry.roll)
      )
      poseBones.add(entry.bone)
    }

    // 新しいポーズにないボーンは identity に戻す
    for (const bone of this._allUsedBones) {
      if (!poseBones.has(bone)) {
        newTargets.set(bone, new THREE.Quaternion())
      }
    }

    this._targetRotations = newTargets
  }

  /** 内部補間を進める（毎フレーム呼び出し） */
  public update(delta: number): void {
    this._elapsedTime += delta

    for (const [bone, target] of this._targetRotations) {
      let current = this._currentRotations.get(bone)
      if (!current) {
        current = new THREE.Quaternion()
        this._currentRotations.set(bone, current)
      }
      current.slerp(target, 1 - Math.exp(-GESTURE_SLERP_SPEED * delta))
    }
  }

  /**
   * ボーンにジェスチャー回転を加算適用する
   * mixer.update() の後、vrm.update() の前に呼ぶこと
   */
  public applyGesture(vrm: VRM): void {
    const humanoid = vrm.humanoid
    if (!humanoid) return

    // 1. 全対象ボーンをベースクォータニオンにリセット
    //    （mixer がアニメーションしないボーンでの回転累積を防止）
    const allBones: VRMHumanBoneName[] = [
      ...this._currentRotations.keys(),
      BREATHING.bone,
      IDLE_SWAY.bone,
    ]

    for (const boneName of allBones) {
      const boneNode = humanoid.getNormalizedBoneNode(boneName)
      if (!boneNode) continue

      if (!this._baseQuats.has(boneName)) {
        this._baseQuats.set(boneName, boneNode.quaternion.clone())
      }
      boneNode.quaternion.copy(this._baseQuats.get(boneName)!)
    }

    // 2. ジェスチャーオフセットを乗算
    for (const [boneName, offset] of this._currentRotations) {
      const boneNode = humanoid.getNormalizedBoneNode(boneName)
      if (!boneNode) continue
      boneNode.quaternion.multiply(offset)
    }

    // 3. オシレーション（呼吸・揺れ）を乗算
    this._applyOscillation(
      humanoid,
      BREATHING.bone,
      BREATHING.axis,
      BREATHING.amplitudeDeg,
      BREATHING.frequencyHz
    )
    this._applyOscillation(
      humanoid,
      IDLE_SWAY.bone,
      IDLE_SWAY.axis,
      IDLE_SWAY.amplitudeDeg,
      IDLE_SWAY.frequencyHz
    )
  }

  private _applyOscillation(
    humanoid: VRM['humanoid'],
    bone: VRMHumanBoneName,
    axis: 'pitch' | 'yaw' | 'roll',
    amplitudeDeg: number,
    frequencyHz: number
  ): void {
    const boneNode = humanoid.getNormalizedBoneNode(bone)
    if (!boneNode) return

    const angle =
      Math.sin(this._elapsedTime * frequencyHz * Math.PI * 2) * amplitudeDeg

    this._tempEuler.set(
      axis === 'pitch' ? angle * DEG2RAD : 0,
      axis === 'yaw' ? angle * DEG2RAD : 0,
      axis === 'roll' ? angle * DEG2RAD : 0,
      'YXZ'
    )
    this._tempQuat.setFromEuler(this._tempEuler)
    boneNode.quaternion.multiply(this._tempQuat)
  }
}
